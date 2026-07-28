from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path
from typing import Any

from .config.loader import load_settings
from .mcp_server import create_server
from .observability.logging import configure_logging, get_logger
from .osc.transport import OSCTransport
from .osc.receiver import OSCReceiver
from .domain.adapter import VRChatDomainAdapter
from .vrc_config.parser import load_avatar_schema
from .vrc_config.resolver import resolve_avatar_config_path
from .oscquery.client import OscQueryClient
from .singleton import InstanceAlreadyRunningError, SingleInstanceLock


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="vrchat-osc-mcp")

    p.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to YAML config file (default: auto-detect ./config.yaml or ./config/config.yaml)",
    )
    p.add_argument("--transport", choices=["stdio", "sse", "http"], default=None)

    p.add_argument("--osc-send-ip", default=None)
    p.add_argument("--osc-send-port", type=int, default=None)

    p.add_argument("--enable-receiver", action="store_true", help="Enable OSC receiver (MVP-0: disabled by default)")
    p.add_argument("--no-receiver", action="store_true", help="Disable OSC receiver")
    p.add_argument("--osc-receive-ip", default=None)
    p.add_argument("--osc-receive-port", type=int, default=None)

    p.add_argument("--sse-host", default=None)
    p.add_argument("--sse-port", type=int, default=None)

    p.add_argument("--http-host", default=None)
    p.add_argument("--http-port", type=int, default=None)
    p.add_argument("--http-path", default=None)

    p.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default=None)

    p.add_argument("--vrchat-osc-root", type=Path, default=None)
    p.add_argument(
        "--avatar-config",
        type=Path,
        default=None,
        help="Explicit path to Avatar OSC config JSON (avtr_*.json); used for strict schema validation",
    )

    return p


def _cli_overrides(args: argparse.Namespace) -> dict[str, Any]:
    o: dict[str, Any] = {}

    if args.transport is not None:
        o.setdefault("mcp", {})["transport"] = args.transport

    if args.sse_host is not None or args.sse_port is not None:
        o.setdefault("mcp", {}).setdefault("sse", {})
        if args.sse_host is not None:
            o["mcp"]["sse"]["host"] = args.sse_host
        if args.sse_port is not None:
            o["mcp"]["sse"]["port"] = args.sse_port

    if args.http_host is not None or args.http_port is not None or args.http_path is not None:
        o.setdefault("mcp", {}).setdefault("http", {})
        if args.http_host is not None:
            o["mcp"]["http"]["host"] = args.http_host
        if args.http_port is not None:
            o["mcp"]["http"]["port"] = args.http_port
        if args.http_path is not None:
            o["mcp"]["http"]["path"] = args.http_path

    if args.osc_send_ip is not None or args.osc_send_port is not None:
        o.setdefault("osc", {}).setdefault("send", {})
        if args.osc_send_ip is not None:
            o["osc"]["send"]["ip"] = args.osc_send_ip
        if args.osc_send_port is not None:
            o["osc"]["send"]["port"] = args.osc_send_port

    # Receiver tri-state: CLI overrides YAML when explicitly specified.
    if args.enable_receiver and args.no_receiver:
        raise SystemExit("--enable-receiver and --no-receiver cannot be used together")

    if args.enable_receiver or args.no_receiver or args.osc_receive_ip is not None or args.osc_receive_port is not None:
        o.setdefault("osc", {}).setdefault("receive", {})
        if args.enable_receiver:
            o["osc"]["receive"]["enabled"] = True
        if args.no_receiver:
            o["osc"]["receive"]["enabled"] = False
        if args.osc_receive_ip is not None:
            o["osc"]["receive"]["ip"] = args.osc_receive_ip
        if args.osc_receive_port is not None:
            o["osc"]["receive"]["port"] = args.osc_receive_port

    if args.log_level is not None:
        o.setdefault("logging", {})["level"] = args.log_level

    if args.vrchat_osc_root is not None:
        o.setdefault("vrchat", {})["osc_root"] = str(args.vrchat_osc_root)

    if args.avatar_config is not None:
        o.setdefault("vrchat", {})["avatar_config"] = str(args.avatar_config)

    return o


def _vrchat_is_running() -> bool:
    try:
        import psutil
    except ImportError:
        return False
    try:
        return any(
            (proc.info.get("name") or "").lower() == "vrchat.exe"
            for proc in psutil.process_iter(["name"])
        )
    except Exception:  # noqa: BLE001
        return False


async def _vrchat_idle_watchdog(*, idle_timeout_s: float, poll_interval_s: float, logger) -> None:
    """Return once VRChat.exe hasn't been seen running for idle_timeout_s.

    Runs alongside the MCP server so an unattended instance doesn't sit
    around indefinitely (and doing the psutil check is far cheaper than the
    OSCQuery discovery path, so this alone doesn't add to background CPU use).
    """
    last_seen = time.monotonic()
    while True:
        await asyncio.sleep(poll_interval_s)
        now = time.monotonic()
        if _vrchat_is_running():
            last_seen = now
            continue
        idle_s = now - last_seen
        if idle_s >= idle_timeout_s:
            logger.warning("watchdog.vrchat_idle_shutdown", idle_s=int(idle_s))
            return


async def _mcp_idle_watchdog(*, activity, idle_timeout_s: float, poll_interval_s: float, logger) -> None:
    """Return once no MCP request/notification has been seen for idle_timeout_s.

    Safety net independent of the stdio transport's own EOF detection: a
    disconnected client should make the stdio read loop end server_task on
    its own, but a multi-layer process supervisor (e.g. `uv run` wrapping a
    venv console script) does not always propagate that promptly on Windows,
    which can otherwise leave this process (and any running tracking/eye
    streams) running indefinitely with no client attached.
    """
    while True:
        await asyncio.sleep(poll_interval_s)
        idle_s = time.monotonic() - activity.last_activity_monotonic
        if idle_s >= idle_timeout_s:
            logger.warning("watchdog.mcp_idle_shutdown", idle_s=int(idle_s))
            return


async def _run(settings) -> None:
    configure_logging(level=settings.logging.level, json_logs=settings.logging.json_logs)
    logger = get_logger().bind(component="app")

    schema = None
    schema_source: str | None = None
    schema_path_str: str | None = None
    schema_path = resolve_avatar_config_path(
        osc_root=settings.vrchat.osc_root,
        explicit_path=settings.vrchat.avatar_config,
    )
    if schema_path is not None:
        try:
            schema = load_avatar_schema(schema_path)
            schema_path_str = str(schema_path)
            schema_source = "local_config_explicit" if settings.vrchat.avatar_config is not None else "local_config_newest"
            logger.info(
                "schema.loaded",
                schema_source=schema_source,
                schema_path=schema_path_str,
                avatar_id=schema.avatar_id,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "schema.load_failed",
                schema_source="local_config",
                schema_path=str(schema_path),
                error=str(e),
            )

    osc = OSCTransport(
        send_ip=settings.osc.send.ip,
        send_port=settings.osc.send.port,
        osc_per_second=settings.safety.osc_per_second,
        logger=get_logger().bind(component="osc"),
    )
    await osc.start()

    oscquery = OscQueryClient()
    await oscquery.start()

    adapter = VRChatDomainAdapter(
        transport=osc,
        settings=settings,
        logger=get_logger().bind(component="domain"),
        schema=schema,
        schema_source=schema_source,
        schema_path=schema_path_str,
        oscquery_client=oscquery,
    )

    mcp, mcp_activity = create_server(adapter=adapter)

    receiver: OSCReceiver | None = None
    if settings.osc.receive.enabled:
        receiver = OSCReceiver(
            bind_ip=settings.osc.receive.ip,
            port=settings.osc.receive.port,
            logger=get_logger().bind(component="osc-receiver"),
            on_avatar_change=adapter.on_avatar_change,
        )
        try:
            await receiver.start()
        except Exception as e:  # noqa: BLE001
            # Receiver is optional; do not fail the whole server.
            receiver = None
            logger.warning(
                "osc.receiver.start_failed",
                bind_ip=settings.osc.receive.ip,
                bind_port=settings.osc.receive.port,
                error=str(e),
                hint="This port may be in use by another OSC tool; you can disable the receiver or use a different port.",
            )

    logger.info(
        "server.start",
        transport=settings.mcp.transport,
        osc_send_ip=settings.osc.send.ip,
        osc_send_port=settings.osc.send.port,
        sse_host=settings.mcp.sse.host,
        sse_port=settings.mcp.sse.port,
        http_host=settings.mcp.http.host,
        http_port=settings.mcp.http.port,
        http_path=settings.mcp.http.path,
    )

    async def _serve() -> None:
        if settings.mcp.transport == "stdio":
            await mcp.run_async(transport="stdio")
        elif settings.mcp.transport == "sse":
            await mcp.run_async(transport="sse", host=settings.mcp.sse.host, port=settings.mcp.sse.port)
        else:
            await mcp.run_async(
                transport="http",
                host=settings.mcp.http.host,
                port=settings.mcp.http.port,
                path=settings.mcp.http.path,
            )

    server_task = asyncio.create_task(_serve(), name="mcp-serve")
    tasks: list[asyncio.Task[None]] = [server_task]

    idle_minutes = settings.safety.vrchat_idle_shutdown_minutes
    watchdog_task: asyncio.Task[None] | None = None
    if idle_minutes > 0:
        watchdog_task = asyncio.create_task(
            _vrchat_idle_watchdog(
                idle_timeout_s=idle_minutes * 60.0,
                poll_interval_s=60.0,
                logger=get_logger().bind(component="watchdog"),
            ),
            name="vrchat-idle-watchdog",
        )
        tasks.append(watchdog_task)

    mcp_idle_minutes = settings.safety.mcp_idle_shutdown_minutes
    mcp_watchdog_task: asyncio.Task[None] | None = None
    if mcp_idle_minutes > 0:
        mcp_watchdog_task = asyncio.create_task(
            _mcp_idle_watchdog(
                activity=mcp_activity,
                idle_timeout_s=mcp_idle_minutes * 60.0,
                poll_interval_s=60.0,
                logger=get_logger().bind(component="watchdog"),
            ),
            name="mcp-idle-watchdog",
        )
        tasks.append(mcp_watchdog_task)

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        if watchdog_task is not None and watchdog_task in done:
            logger.info("server.stop", reason="vrchat_idle_timeout", idle_minutes=idle_minutes)
        elif mcp_watchdog_task is not None and mcp_watchdog_task in done:
            logger.info("server.stop", reason="mcp_idle_timeout", idle_minutes=mcp_idle_minutes)
        elif server_task in done:
            exc = server_task.exception()
            if exc is not None:
                raise exc
    finally:
        # Best-effort: stop any running background streams (tracking/eye) and
        # release held buttons/typing before tearing down transports, so a
        # server shutdown for any reason (client disconnect, either idle
        # watchdog) doesn't leave them orphaned mid-loop.
        try:
            await adapter.stop_all(trace_id="server_shutdown")
        except Exception as e:  # noqa: BLE001
            logger.warning("shutdown.stop_all_failed", error=str(e))
        if receiver is not None:
            await receiver.close()
        await oscquery.stop()
        await osc.close()


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[1]
    loaded = load_settings(project_root=project_root, config_path=args.config, cli_overrides=_cli_overrides(args))

    lock = SingleInstanceLock()
    try:
        lock.acquire()
    except InstanceAlreadyRunningError as e:
        print(f"[vrchat-osc-mcp] {e}", file=sys.stderr)
        return 1

    try:
        asyncio.run(_run(loaded.settings))
    finally:
        lock.release()
    return 0
