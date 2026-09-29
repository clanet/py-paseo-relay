#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, WSMsgType


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Smoke-test the local py-relay implementation.",
    )
    parser.add_argument(
        "--base-url",
        help="Test an already-running relay at this base URL instead of spawning relay.py.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for a spawned relay to become healthy.",
    )
    parser.add_argument(
        "--relay-log-level",
        default="INFO",
        help="Log level to use when spawning relay.py.",
    )
    return parser


def pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


async def wait_for_health(base_url: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    timeout = ClientTimeout(total=2)

    while True:
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(f"{base_url}/health") as response:
                    if response.status == 200 and await response.json() == {"status": "ok"}:
                        return
        except Exception:
            pass

        if time.monotonic() >= deadline:
            raise TimeoutError(f"Relay did not become healthy within {timeout_seconds:.1f}s")
        await asyncio.sleep(0.1)


async def recv_json(ws) -> dict[str, object]:
    message = await ws.receive(timeout=5)
    if message.type != WSMsgType.TEXT:
        raise AssertionError(f"Expected text WebSocket message, got {message.type!r}")
    payload = json.loads(message.data)
    if not isinstance(payload, dict):
        raise AssertionError(f"Expected JSON object, got {payload!r}")
    return payload


async def run_smoke_test(base_url: str) -> None:
    server_id = f"srv_smoke_{uuid.uuid4().hex[:8]}"

    async with ClientSession() as session:
        async with session.get(f"{base_url}/health") as response:
            assert response.status == 200, response.status
            assert await response.json() == {"status": "ok"}

        control = await session.ws_connect(f"{base_url}/ws?role=server&serverId={server_id}&v=2")
        assert await recv_json(control) == {"type": "sync", "connectionIds": []}

        client = await session.ws_connect(f"{base_url}/ws?role=client&serverId={server_id}&v=2")
        connected = await recv_json(control)
        assert connected.get("type") == "connected", connected
        connection_id = str(connected["connectionId"])
        assert connection_id.startswith("conn_"), connection_id

        await client.send_str("hello")

        data_ws = await session.ws_connect(
            f"{base_url}/ws?role=server&serverId={server_id}&v=2&connectionId={connection_id}"
        )

        message = await data_ws.receive(timeout=5)
        assert message.type == WSMsgType.TEXT, message
        assert message.data == "hello", message.data

        await data_ws.send_str("world")
        message = await client.receive(timeout=5)
        assert message.type == WSMsgType.TEXT, message
        assert message.data == "world", message.data

        await control.send_str(json.dumps({"type": "ping"}))
        pong = await recv_json(control)
        assert pong.get("type") == "pong", pong
        assert isinstance(pong.get("ts"), int), pong

        await client.close()
        assert await recv_json(control) == {
            "type": "disconnected",
            "connectionId": connection_id,
        }

        await data_ws.close()
        await control.close()


async def run_oversized_pending_frame_test(base_url: str) -> None:
    server_id = f"srv_pending_{uuid.uuid4().hex[:8]}"
    oversized = b"x" * (int(1.25 * 1024 * 1024))

    async with ClientSession() as session:
        control = await session.ws_connect(f"{base_url}/ws?role=server&serverId={server_id}&v=2")
        assert await recv_json(control) == {"type": "sync", "connectionIds": []}

        client = await session.ws_connect(f"{base_url}/ws?role=client&serverId={server_id}&v=2")
        connected = await recv_json(control)
        assert connected.get("type") == "connected", connected

        await client.send_bytes(oversized)

        close_message = await client.receive(timeout=5)
        assert close_message.type in {
            WSMsgType.CLOSE,
            WSMsgType.CLOSING,
            WSMsgType.CLOSED,
        }, close_message.type

        assert await recv_json(control) == {
            "type": "disconnected",
            "connectionId": connected["connectionId"],
        }

        await control.close()


def spawn_relay(log_level: str, extra_args: list[str] | None = None) -> tuple[subprocess.Popen[str], str]:
    relay_path = Path(__file__).with_name("relay.py")
    port = pick_free_port()
    base_url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [
            sys.executable,
            str(relay_path),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            log_level,
            *(extra_args or []),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, base_url


def collect_process_output(process: subprocess.Popen[str]) -> str:
    try:
        stdout, _ = process.communicate(timeout=1)
        return stdout or ""
    except subprocess.TimeoutExpired:
        return ""


def stop_process(process: subprocess.Popen[str]) -> str:
    if process.poll() is None:
        process.terminate()
        try:
            stdout, _ = process.communicate(timeout=5)
            return stdout or ""
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, _ = process.communicate(timeout=5)
            return stdout or ""
    return collect_process_output(process)


async def async_main(args: argparse.Namespace) -> int:
    process: subprocess.Popen[str] | None = None
    limit_process: subprocess.Popen[str] | None = None
    spawned_output = ""
    limit_process_output = ""

    try:
        if args.base_url:
            base_url = args.base_url.rstrip("/")
            print(f"[smoke] testing existing relay at {base_url}")
        else:
            process, base_url = spawn_relay(args.relay_log_level)
            print(f"[smoke] spawned relay.py at {base_url}")
            await wait_for_health(base_url, args.startup_timeout)

        await run_smoke_test(base_url)

        if not args.base_url:
            limit_process, limit_base_url = spawn_relay(
                args.relay_log_level,
                ["--max-pending-bytes-mib", "1", "--max-msg-size-mib", "2"],
            )
            print(f"[smoke] spawned relay.py pending-limit probe at {limit_base_url}")
            await wait_for_health(limit_base_url, args.startup_timeout)
            await run_oversized_pending_frame_test(limit_base_url)

        print("[smoke] ok")
        return 0
    except Exception as error:
        if process is not None:
            spawned_output = stop_process(process)
            process = None
        if limit_process is not None:
            limit_process_output = stop_process(limit_process)
            limit_process = None
        print(f"[smoke] failed: {error}", file=sys.stderr)
        if spawned_output.strip():
            print("\n[relay output]\n" + spawned_output.rstrip(), file=sys.stderr)
        if limit_process_output.strip():
            print("\n[relay pending-limit output]\n" + limit_process_output.rstrip(), file=sys.stderr)
        return 1
    finally:
        if process is not None:
            stop_process(process)
        if limit_process is not None:
            stop_process(limit_process)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
