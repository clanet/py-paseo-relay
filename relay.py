#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque

from aiohttp import WSMsgType, web

CURRENT_PROTOCOL_VERSION = "2"
DEFAULT_HEARTBEAT_SECONDS = 20.0
DEFAULT_MAX_PENDING_FRAMES = 200
DEFAULT_MAX_MESSAGE_SIZE_MIB = 64
DEFAULT_MAX_PENDING_BYTES = DEFAULT_MAX_MESSAGE_SIZE_MIB * 1024 * 1024
DEFAULT_MAX_SESSIONS = 10_000

CONTROL_NUDGE_DELAY_SECONDS = 10.0
CONTROL_RESET_DELAY_SECONDS = 5.0

CLIENT_DISCONNECTED = 1001
POLICY_VIOLATION = 1008
MESSAGE_TOO_BIG = 1009
INTERNAL_ERROR = 1011
SERVER_DISCONNECTED = 1012


def maybe_install_uvloop() -> None:
    if sys.platform.startswith("win"):
        return
    try:
        import uvloop  # type: ignore
    except ImportError:
        return
    uvloop.install()


@dataclass(slots=True, frozen=True)
class Frame:
    kind: str
    data: str | bytes
    size: int

    @classmethod
    def from_text(cls, data: str) -> "Frame":
        return cls(kind="text", data=data, size=len(data.encode("utf-8")))

    @classmethod
    def from_binary(cls, data: bytes) -> "Frame":
        return cls(kind="binary", data=data, size=len(data))


@dataclass(slots=True, frozen=True)
class ConnectionMeta:
    server_id: str
    role: str
    connection_id: str | None
    version: str

    @property
    def is_control(self) -> bool:
        return self.role == "server" and self.connection_id is None


@dataclass(slots=True, eq=False)
class RelaySocket:
    ws: web.WebSocketResponse
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(slots=True, eq=False)
class Pipe:
    connection_id: str
    server_data: RelaySocket | None = None
    clients: set[RelaySocket] = field(default_factory=set)
    pending_frames: Deque[Frame] = field(default_factory=deque)
    pending_bytes: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def is_empty_unlocked(self) -> bool:
        return self.server_data is None and not self.clients and not self.pending_frames


@dataclass(slots=True, eq=False)
class Session:
    server_id: str
    control: RelaySocket | None = None
    control_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pipes: dict[str, Pipe] = field(default_factory=dict)
    pipes_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    nudge_tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict)
    handler_count: int = 0


class RelayManager:
    def __init__(
        self,
        logger: logging.Logger,
        max_pending_frames: int,
        max_pending_bytes: int,
        max_sessions: int,
    ) -> None:
        self._logger = logger
        self._max_pending_frames = max_pending_frames
        self._max_pending_bytes = max_pending_bytes
        self._max_sessions = max_sessions
        self._sessions: dict[str, Session] = {}
        self._sessions_lock = asyncio.Lock()

    async def acquire_session(self, server_id: str) -> Session | None:
        async with self._sessions_lock:
            session = self._sessions.get(server_id)
            if session is None:
                if len(self._sessions) >= self._max_sessions:
                    return None
                session = Session(server_id=server_id)
                self._sessions[server_id] = session
            session.handler_count += 1
            return session

    async def release_session(self, session: Session) -> None:
        async with self._sessions_lock:
            current = self._sessions.get(session.server_id)
            if current is not session:
                return
            if session.handler_count > 0:
                session.handler_count -= 1
            if session.handler_count == 0 and session.control is None and not session.pipes:
                self._sessions.pop(session.server_id, None)

    async def close_all(self) -> None:
        async with self._sessions_lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()

        results = await asyncio.gather(
            *(self._shutdown_session(s) for s in sessions),
            return_exceptions=True,
        )
        close_coros = [
            coro for r in results if isinstance(r, list) for coro in r
        ]
        if close_coros:
            await asyncio.gather(*close_coros, return_exceptions=True)

    async def register(self, session: Session, socket: RelaySocket, meta: ConnectionMeta) -> None:
        if meta.is_control:
            await self._register_control(session, socket)
            return
        if meta.role == "server":
            assert meta.connection_id is not None
            await self._register_server_data(session, socket, meta.connection_id)
            return

        assert meta.connection_id is not None
        await self._register_client(session, socket, meta.connection_id)

    async def handle_frame(
        self,
        session: Session,
        socket: RelaySocket,
        meta: ConnectionMeta,
        frame: Frame,
    ) -> None:
        if meta.is_control:
            control = await self._get_control(session)
            if control is not None:
                await self._handle_control_frame(control, frame)
            return

        assert meta.connection_id is not None
        if meta.role == "client":
            await self._handle_client_frame(session, socket, meta.connection_id, frame)
            return

        await self._handle_server_frame(session, meta.connection_id, frame)

    async def unregister(self, session: Session, socket: RelaySocket, meta: ConnectionMeta) -> None:
        if meta.is_control:
            await self._clear_control_if_same(session, socket)
            return

        assert meta.connection_id is not None
        if meta.role == "server":
            await self._unregister_server_data(session, socket, meta.connection_id)
            return

        await self._unregister_client(session, socket, meta.connection_id)

    async def _register_control(self, session: Session, socket: RelaySocket) -> None:
        replaced = await self._swap_control(session, socket)
        if replaced is not None and replaced is not socket:
            await safe_close(replaced, POLICY_VIOLATION, "Replaced by new connection")

        delivered = await self._send_json(
            socket,
            {
                "type": "sync",
                "connectionIds": await self._connected_connection_ids(session),
            },
        )
        if not delivered and await self._clear_control_if_same(session, socket):
            await safe_close(socket, INTERNAL_ERROR, "Initial sync failed")
            return

        self._logger.info("relay control connected server_id=%s", session.server_id)

    async def _register_server_data(
        self,
        session: Session,
        socket: RelaySocket,
        connection_id: str,
    ) -> None:
        pipe = await self._get_or_create_pipe(session, connection_id)
        async with pipe.lock:
            replaced = pipe.server_data
            pipe.server_data = socket
            queued_frames = list(pipe.pending_frames)
            pipe.pending_frames.clear()
            pipe.pending_bytes = 0

        if replaced is not None and replaced is not socket:
            await safe_close(replaced, POLICY_VIOLATION, "Replaced by new connection")

        await self._cancel_nudge(session, connection_id)

        if queued_frames:
            await self._flush_pending_frames(session, pipe, socket, queued_frames)

        self._logger.info(
            "relay data connected server_id=%s connection_id=%s",
            session.server_id,
            connection_id,
        )

    async def _register_client(
        self,
        session: Session,
        socket: RelaySocket,
        connection_id: str,
    ) -> None:
        pipe = await self._get_or_create_pipe(session, connection_id)
        async with pipe.lock:
            pipe.clients.add(socket)

        await self._notify_control(
            session,
            {
                "type": "connected",
                "connectionId": connection_id,
            },
        )
        await self._schedule_nudge(session, connection_id)
        self._logger.info(
            "relay client connected server_id=%s connection_id=%s",
            session.server_id,
            connection_id,
        )

    async def _unregister_server_data(
        self,
        session: Session,
        socket: RelaySocket,
        connection_id: str,
    ) -> None:
        pipe = await self._get_pipe(session, connection_id)
        if pipe is None:
            return

        async with pipe.lock:
            if pipe.server_data is not socket:
                return
            pipe.server_data = None
            clients = tuple(pipe.clients)

        if clients:
            await asyncio.gather(
                *(safe_close(client, SERVER_DISCONNECTED, "Server disconnected") for client in clients),
                return_exceptions=True,
            )

        await self._cancel_nudge(session, connection_id)
        await self._remove_pipe_if_empty(session, connection_id, pipe)

    async def _unregister_client(
        self,
        session: Session,
        socket: RelaySocket,
        connection_id: str,
    ) -> None:
        pipe = await self._get_pipe(session, connection_id)
        if pipe is None:
            return

        is_last_client = False
        server_data: RelaySocket | None = None

        async with pipe.lock:
            if socket not in pipe.clients:
                return
            pipe.clients.remove(socket)
            if not pipe.clients:
                is_last_client = True
                pipe.pending_frames.clear()
                pipe.pending_bytes = 0
                server_data = pipe.server_data
                pipe.server_data = None

        if not is_last_client:
            return

        await self._cancel_nudge(session, connection_id)

        coros: list[object] = [
            self._notify_control(
                session,
                {"type": "disconnected", "connectionId": connection_id},
            )
        ]
        if server_data is not None:
            coros.append(safe_close(server_data, CLIENT_DISCONNECTED, "Client disconnected"))
        await asyncio.gather(*coros, return_exceptions=True)

        await self._remove_pipe_if_empty(session, connection_id, pipe)

    async def _handle_control_frame(self, socket: RelaySocket, frame: Frame) -> None:
        if frame.kind != "text":
            return

        try:
            payload = json.loads(frame.data)
        except (TypeError, json.JSONDecodeError):
            return

        if not isinstance(payload, dict) or payload.get("type") != "ping":
            return

        await self._send_json(socket, {"type": "pong", "ts": now_millis()})

    async def _handle_client_frame(
        self,
        session: Session,
        socket: RelaySocket,
        connection_id: str,
        frame: Frame,
    ) -> None:
        pipe = await self._get_or_create_pipe(session, connection_id)
        async with pipe.lock:
            target = pipe.server_data
            if target is None:
                buffered = self._buffer_frame_unlocked(pipe, frame)
        if target is None:
            if not buffered:
                await self._close_buffer_overflow(socket, session, connection_id, frame)
            return
        # 服务端数据通道已连接，直接转发
        if await send_frame(target, frame):
            return

        await safe_close(target, INTERNAL_ERROR, "Forward failed")
        async with pipe.lock:
            if pipe.server_data is target:
                pipe.server_data = None
            buffered = self._buffer_frame_unlocked(pipe, frame)
        if buffered:
            await self._schedule_nudge(session, connection_id)
        else:
            await self._close_buffer_overflow(socket, session, connection_id, frame)

    async def _close_buffer_overflow(
        self,
        socket: RelaySocket,
        session: Session,
        connection_id: str,
        frame: Frame,
    ) -> None:
        self._logger.warning(
            "relay closing client: buffer overflow server_id=%s connection_id=%s frame_bytes=%s max_pending_bytes=%s",
            session.server_id,
            connection_id,
            frame.size,
            self._max_pending_bytes,
        )
        await safe_close(socket, MESSAGE_TOO_BIG, "Frame exceeds pending buffer limit")

    async def _handle_server_frame(
        self,
        session: Session,
        connection_id: str,
        frame: Frame,
    ) -> None:
        pipe = await self._get_pipe(session, connection_id)
        if pipe is None:
            return

        async with pipe.lock:
            targets = tuple(pipe.clients)

        if not targets:
            return

        send_results = await asyncio.gather(
            *(send_frame(client, frame) for client in targets),
            return_exceptions=True,
        )

        failing_clients = [
            client
            for client, result in zip(targets, send_results)
            if result is not True
        ]
        if failing_clients:
            await asyncio.gather(
                *(safe_close(client, SERVER_DISCONNECTED, "Forward failed") for client in failing_clients),
                return_exceptions=True,
            )

    async def _notify_control(self, session: Session, payload: dict[str, object]) -> None:
        target = await self._get_control(session)
        if target is None:
            return

        delivered = await self._send_json(target, payload)
        if delivered:
            return

        if await self._clear_control_if_same(session, target):
            await safe_close(target, INTERNAL_ERROR, "Control send failed")

    async def _flush_pending_frames(
        self,
        session: Session,
        pipe: Pipe,
        socket: RelaySocket,
        frames: list[Frame],
    ) -> None:
        for index, frame in enumerate(frames):
            if await send_frame(socket, frame):
                continue

            await safe_close(socket, INTERNAL_ERROR, "Pending flush failed")
            async with pipe.lock:
                if pipe.server_data is socket:
                    pipe.server_data = None
                for remaining in frames[index:]:
                    self._buffer_frame_unlocked(pipe, remaining)
            await self._schedule_nudge(session, pipe.connection_id)
            return

    def _buffer_frame_unlocked(self, pipe: Pipe, frame: Frame) -> bool:
        if frame.size > self._max_pending_bytes:
            return False

        while pipe.pending_frames and (
            pipe.pending_bytes + frame.size > self._max_pending_bytes
            or len(pipe.pending_frames) >= self._max_pending_frames
        ):
            dropped = pipe.pending_frames.popleft()
            pipe.pending_bytes -= dropped.size

        if len(pipe.pending_frames) >= self._max_pending_frames:
            return False

        pipe.pending_frames.append(frame)
        pipe.pending_bytes += frame.size
        return True

    async def _connected_connection_ids(self, session: Session) -> list[str]:
        async with session.pipes_lock:
            items = tuple(session.pipes.items())

        out: list[str] = []
        for connection_id, pipe in items:
            async with pipe.lock:
                if pipe.clients:
                    out.append(connection_id)
        out.sort()
        return out

    async def _swap_control(
        self,
        session: Session,
        socket: RelaySocket,
    ) -> RelaySocket | None:
        async with session.control_lock:
            replaced = session.control
            session.control = socket
            return replaced

    async def _get_control(self, session: Session) -> RelaySocket | None:
        async with session.control_lock:
            return session.control

    async def _clear_control_if_same(
        self,
        session: Session,
        socket: RelaySocket,
    ) -> bool:
        async with session.control_lock:
            if session.control is not socket:
                return False
            session.control = None
            return True

    async def _get_or_create_pipe(self, session: Session, connection_id: str) -> Pipe:
        async with session.pipes_lock:
            pipe = session.pipes.get(connection_id)
            if pipe is None:
                pipe = Pipe(connection_id=connection_id)
                session.pipes[connection_id] = pipe
            return pipe

    async def _get_pipe(self, session: Session, connection_id: str) -> Pipe | None:
        async with session.pipes_lock:
            return session.pipes.get(connection_id)

    async def _remove_pipe_if_empty(self, session: Session, connection_id: str, pipe: Pipe) -> None:
        async with session.pipes_lock:
            if session.pipes.get(connection_id) is not pipe:
                return
            async with pipe.lock:
                if not pipe.is_empty_unlocked():
                    return
            session.pipes.pop(connection_id, None)

    async def _schedule_nudge(self, session: Session, connection_id: str) -> None:
        task = asyncio.create_task(
            self._nudge_control_task(session, connection_id),
            name=f"relay-nudge:{session.server_id}:{connection_id}",
        )
        async with session.pipes_lock:
            previous = session.nudge_tasks.get(connection_id)
            session.nudge_tasks[connection_id] = task
        if previous is not None:
            previous.cancel()

    async def _cancel_nudge(self, session: Session, connection_id: str) -> None:
        async with session.pipes_lock:
            task = session.nudge_tasks.pop(connection_id, None)
        if task is not None:
            task.cancel()

    async def _pipe_awaiting_server(self, session: Session, connection_id: str) -> bool:
        """客户端已连接但服务端数据通道尚未建立"""
        pipe = await self._get_pipe(session, connection_id)
        if pipe is None:
            return False
        async with pipe.lock:
            return bool(pipe.clients) and pipe.server_data is None

    async def _nudge_control_task(self, session: Session, connection_id: str) -> None:
        current_task = asyncio.current_task()
        try:
            await asyncio.sleep(CONTROL_NUDGE_DELAY_SECONDS)
            if not await self._pipe_awaiting_server(session, connection_id):
                return

            await self._notify_control(
                session,
                {
                    "type": "sync",
                    "connectionIds": await self._connected_connection_ids(session),
                },
            )
            captured_control = await self._get_control(session)

            await asyncio.sleep(CONTROL_RESET_DELAY_SECONDS)
            if not await self._pipe_awaiting_server(session, connection_id) or captured_control is None:
                return

            if await self._clear_control_if_same(session, captured_control):
                await safe_close(captured_control, INTERNAL_ERROR, "Control unresponsive")
        except asyncio.CancelledError:
            raise
        finally:
            async with session.pipes_lock:
                if current_task is not None and session.nudge_tasks.get(connection_id) is current_task:
                    session.nudge_tasks.pop(connection_id, None)

    async def _send_json(self, socket: RelaySocket, payload: dict[str, object]) -> bool:
        return await send_frame(
            socket,
            Frame.from_text(json.dumps(payload, separators=(",", ":"))),
        )

    async def _shutdown_session(self, session: Session) -> list[object]:
        async with session.pipes_lock:
            nudge_tasks = list(session.nudge_tasks.values())
            session.nudge_tasks.clear()
            pipes = list(session.pipes.values())
            session.pipes.clear()

        for task in nudge_tasks:
            task.cancel()

        async with session.control_lock:
            control = session.control
            session.control = None

        close_coroutines = []
        if control is not None:
            close_coroutines.append(safe_close(control, CLIENT_DISCONNECTED, "Relay shutting down"))

        for pipe in pipes:
            async with pipe.lock:
                server_data = pipe.server_data
                pipe.server_data = None
                clients = tuple(pipe.clients)
                pipe.clients.clear()
                pipe.pending_frames.clear()
                pipe.pending_bytes = 0

            if server_data is not None:
                close_coroutines.append(
                    safe_close(server_data, CLIENT_DISCONNECTED, "Relay shutting down")
                )
            for client in clients:
                close_coroutines.append(safe_close(client, CLIENT_DISCONNECTED, "Relay shutting down"))

        return close_coroutines


async def send_frame(socket: RelaySocket, frame: Frame) -> bool:
    ws = socket.ws
    if ws.closed:
        return False

    try:
        async with socket.write_lock:
            if ws.closed:
                return False
            if frame.kind == "binary":
                await ws.send_bytes(frame.data)  # type: ignore[arg-type]
            else:
                await ws.send_str(frame.data)  # type: ignore[arg-type]
        return True
    except (ConnectionError, RuntimeError, TypeError, ValueError):
        return False


async def safe_close(socket: RelaySocket, code: int, reason: str) -> None:
    ws = socket.ws
    if ws.closed:
        return
    with contextlib.suppress(ConnectionError, RuntimeError, ValueError):
        async with socket.write_lock:
            if ws.closed:
                return
            await ws.close(code=code, message=reason.encode("utf-8", errors="ignore"))


def now_millis() -> int:
    return time.time_ns() // 1_000_000


def parse_version(raw: str | None) -> str:
    if raw is None:
        return CURRENT_PROTOCOL_VERSION
    value = raw.strip()
    if not value:
        return CURRENT_PROTOCOL_VERSION
    if value != CURRENT_PROTOCOL_VERSION:
        raise web.HTTPBadRequest(text="Invalid v parameter (expected 2)")
    return value


def parse_role(raw: str | None) -> str:
    if raw not in {"server", "client"}:
        raise web.HTTPBadRequest(text="Missing or invalid role parameter")
    return raw


def parse_server_id(raw: str | None) -> str:
    value = (raw or "").strip()
    if not value:
        raise web.HTTPBadRequest(text="Missing serverId parameter")
    return value


def parse_connection_id(raw: str | None) -> str:
    return (raw or "").strip()


def generate_connection_id() -> str:
    return f"conn_{uuid.uuid4().hex[:16]}"


async def health_handler(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def ws_handler(request: web.Request) -> web.StreamResponse:
    manager: RelayManager = request.app["relay_manager"]
    logger: logging.Logger = request.app["relay_logger"]
    heartbeat: float = request.app["relay_heartbeat"]
    max_msg_size: int = request.app["relay_max_msg_size"]

    role = parse_role(request.query.get("role"))
    server_id = parse_server_id(request.query.get("serverId"))
    version = parse_version(request.query.get("v"))
    connection_id = parse_connection_id(request.query.get("connectionId"))

    if role == "client" and not connection_id:
        connection_id = generate_connection_id()

    meta = ConnectionMeta(
        server_id=server_id,
        role=role,
        connection_id=connection_id or None,
        version=version,
    )
    conn_label = meta.connection_id or "-"

    ws = web.WebSocketResponse(
        heartbeat=heartbeat,
        compress=False,
        max_msg_size=max_msg_size,
        autoping=True,
    )
    await ws.prepare(request)

    socket = RelaySocket(ws=ws)
    session = await manager.acquire_session(server_id)
    if session is None:
        logger.warning("relay rejected connection: max sessions reached server_id=%s", server_id)
        await safe_close(socket, POLICY_VIOLATION, "Too many sessions")
        return ws

    logger.info(
        "relay ws accepted server_id=%s role=%s connection_id=%s",
        server_id, role, conn_label,
    )

    try:
        await manager.register(session, socket, meta)
        async for message in ws:
            if message.type == WSMsgType.TEXT:
                await manager.handle_frame(session, socket, meta, Frame.from_text(message.data))
            elif message.type == WSMsgType.BINARY:
                await manager.handle_frame(
                    session, socket, meta, Frame.from_binary(bytes(message.data)),
                )
            elif message.type == WSMsgType.ERROR:
                logger.warning(
                    "relay ws error server_id=%s role=%s connection_id=%s error=%r",
                    server_id, role, conn_label, ws.exception(),
                )
    finally:
        await manager.unregister(session, socket, meta)
        await manager.release_session(session)
        logger.info(
            "relay ws closed server_id=%s role=%s connection_id=%s",
            server_id, role, conn_label,
        )

    return ws


async def not_found_handler(_request: web.Request) -> web.Response:
    return web.Response(status=404, text="Not found")


async def on_shutdown(app: web.Application) -> None:
    manager: RelayManager = app["relay_manager"]
    await manager.close_all()


def build_app(args: argparse.Namespace) -> web.Application:
    logger = logging.getLogger("py-relay")
    manager = RelayManager(
        logger=logger,
        max_pending_frames=args.max_pending_frames,
        max_pending_bytes=args.max_pending_bytes_mib * 1024 * 1024,
        max_sessions=args.max_sessions,
    )

    app = web.Application()
    app["relay_manager"] = manager
    app["relay_logger"] = logger
    app["relay_heartbeat"] = args.heartbeat
    app["relay_max_msg_size"] = args.max_msg_size_mib * 1024 * 1024
    app.router.add_get("/health", health_handler)
    app.router.add_get("/ws", ws_handler)
    app.router.add_route("*", "/{tail:.*}", not_found_handler)
    app.on_shutdown.append(on_shutdown)
    return app


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Minimal self-hosted relay for the current Paseo v2 protocol.",
    )
    parser.add_argument("--host", default=os.getenv("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8787")))
    parser.add_argument(
        "--heartbeat",
        type=float,
        default=float(os.getenv("PASEO_RELAY_HEARTBEAT", str(DEFAULT_HEARTBEAT_SECONDS))),
        help="WebSocket heartbeat interval in seconds.",
    )
    parser.add_argument(
        "--max-pending-frames",
        type=int,
        default=int(os.getenv("PASEO_RELAY_MAX_PENDING_FRAMES", str(DEFAULT_MAX_PENDING_FRAMES))),
        help="Max buffered client frames per connection before older frames are dropped.",
    )
    parser.add_argument(
        "--max-pending-bytes-mib",
        type=int,
        default=int(
            os.getenv(
                "PASEO_RELAY_MAX_PENDING_BYTES_MIB",
                str(DEFAULT_MAX_PENDING_BYTES // (1024 * 1024)),
            )
        ),
        help="Max buffered bytes per connection while waiting for the daemon data socket.",
    )
    parser.add_argument(
        "--max-msg-size-mib",
        type=int,
        default=int(
            os.getenv("PASEO_RELAY_MAX_MSG_SIZE_MIB", str(DEFAULT_MAX_MESSAGE_SIZE_MIB))
        ),
        help="Max incoming WebSocket message size in MiB.",
    )
    parser.add_argument(
        "--max-sessions",
        type=int,
        default=int(os.getenv("PASEO_RELAY_MAX_SESSIONS", str(DEFAULT_MAX_SESSIONS))),
        help="Max concurrent serverId sessions before rejecting new ones.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Python logging level.",
    )
    return parser


def main() -> None:
    maybe_install_uvloop()
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)

    app = build_app(args)
    web.run_app(
        app,
        host=args.host,
        port=args.port,
        access_log=None,
        backlog=512,
        handle_signals=True,
        reuse_address=True,
    )


if __name__ == "__main__":
    main()
