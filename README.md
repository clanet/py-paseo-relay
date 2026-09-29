# py-relay

A self-hosted Paseo relay implementation for personal use.

It keeps the core behavior required by the current default Paseo relay protocol:

- `GET /health`
- `GET /ws?role=...&serverId=...&v=2&connectionId=...`
- `server-control`, `server-data`, and `client` socket roles
- relay-side `connectionId` assignment
- buffering when clients arrive before the daemon data socket
- `sync`, `connected`, and `disconnected` control messages
- automatic control-channel `sync` / reset when the control path becomes unresponsive

To improve single-node performance, this version also includes:

- optional automatic `uvloop` enablement
- per-pipe locking instead of serializing the entire session hot path
- a dedicated write lock per WebSocket to avoid concurrent write issues
- pending-buffer limits on both frame count and total bytes
- a session cap to avoid unbounded memory growth
- a default pending-buffer size aligned with the max message size, so frames are not silently dropped just because the daemon data socket is not attached yet

It intentionally removes the Cloudflare Workers / Durable Objects layer, which makes it a good fit for:

- personal use
- single-machine deployment
- small-scale concurrency
- setups that do not need multi-node horizontal scaling

## Compatibility

This implementation targets the current default Paseo relay v2 protocol.

In other words, it is meant to work with the current daemon / app / CLI behavior in this repository, not to fully reproduce every Cloudflare-specific platform feature from the upstream relay.

## Requirements

- Python 3.10+
- Linux / macOS / Windows
- if `uvloop` is installed on Linux/macOS, the script enables it automatically

## Install

```bash
cd py-relay
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python relay.py --host 0.0.0.0 --port 8787
```

After startup:

- health check: `http://127.0.0.1:8787/health`
- WebSocket endpoint: `ws://127.0.0.1:8787/ws`

You can also run the one-command smoke test:

```bash
python test_smoke.py
```

If the relay is already running, you can test the existing instance directly:

```bash
python test_smoke.py --base-url http://127.0.0.1:8787
```

If you terminate TLS on port 443, use a public endpoint like:

- `relay.example.com:443`

Paseo will then use:

- `wss://relay.example.com:443/ws`

## Options

```bash
python relay.py \
  --host 0.0.0.0 \
  --port 8787 \
  --heartbeat 20 \
  --max-pending-frames 200 \
  --max-pending-bytes-mib 64 \
  --max-msg-size-mib 64 \
  --max-sessions 10000 \
  --log-level INFO
```

Environment variables are also supported:

- `HOST`
- `PORT`
- `LOG_LEVEL`
- `PASEO_RELAY_HEARTBEAT`
- `PASEO_RELAY_MAX_PENDING_FRAMES`
- `PASEO_RELAY_MAX_PENDING_BYTES_MIB`
- `PASEO_RELAY_MAX_MSG_SIZE_MIB`
- `PASEO_RELAY_MAX_SESSIONS`

## Connect It To Paseo

Point the daemon relay settings at your self-hosted relay.

If your public relay address is `relay.example.com:443`, use:

- `relayEndpoint=relay.example.com:443`
- `relayPublicEndpoint=relay.example.com:443`

For local testing, you can start with:

- `127.0.0.1:8787`

## Reverse Proxy Notes

If you place Nginx or Caddy in front of the relay:

- proxy `/health` as normal HTTP
- enable WebSocket upgrade for `/ws`
- use HTTPS / WSS externally

## Known Tradeoffs

To keep the implementation simple, it does not include these Cloudflare-specific capabilities:

- Durable Objects
- hibernation
- multi-instance session affinity
- cross-node shared state

So it is best suited for:

- a single VPS
- a home NAS
- your own small server

If you later need a large public relay, you can evolve this into a Redis-backed design with sticky sessions and multi-process / multi-node coordination.
