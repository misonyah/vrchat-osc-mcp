"""OSCQuery client for VRChat.

VRChat 2023.4+ exposes an OSCQuery HTTP server on a random TCP port and
advertises it via mDNS (_oscjson._tcp).  This module discovers that port
and fetches the live parameter tree, giving names + types + current values
without needing VRChat to broadcast OSC output first.

Discovery strategy (in order):
1. psutil process lookup (Windows / all platforms) — finds VRChat's PID,
   gets its listening TCP ports, probes each with a quick HTTP check.
   ~200 ms on a loaded system.
2. mDNS fallback via zeroconf — briefly browses for VRChat's _oscjson._tcp
   announcement, bounded by `mdns_timeout_s`. The zeroconf listener is only
   alive for that window, not for the life of the process: leaving it
   running continuously means parsing every local mDNS/Bonjour packet on
   the LAN indefinitely, which is a real (if quiet) background CPU cost
   that shows up even when VRChat was never started.

A failed discovery attempt (including a failed mDNS browse) is cached for
`mdns_negative_cache_s` so repeated status polls don't re-trigger a zeroconf
browse every call. The discovered port itself is cached until `invalidate()`
is called (e.g. after VRChat restarts).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass
class OscQueryParameter:
    path: str    # /avatar/parameters/IsLocal
    type: str    # OSC type string: f i T F N s
    value: Any   # current value reported by VRChat
    access: int  # 1=read 2=write 3=readwrite


class OscQueryClient:
    def __init__(
        self,
        *,
        logger_=None,
        mdns_timeout_s: float = 3.0,
        mdns_negative_cache_s: float = 20.0,
    ) -> None:
        self._log = logger_ or logger
        self._mdns_timeout_s = mdns_timeout_s
        self._mdns_negative_cache_s = mdns_negative_cache_s
        self._port: int | None = None
        self._mdns_last_failed_at: float | None = None

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """No persistent background work to start.

        mDNS discovery is performed lazily and only for a bounded window
        (see `_wait_mdns`) instead of running for the process lifetime.
        """
        return

    async def stop(self) -> None:
        """No persistent background work to stop (kept for API compatibility)."""
        return

    def invalidate(self) -> None:
        """Force re-discovery on next call (e.g. after VRChat restarts)."""
        self._port = None
        self._mdns_last_failed_at = None

    # ── Public API ───────────────────────────────────────────────────────────

    async def get_avatar_id(self) -> str | None:
        """Return the current avatar ID string, or None if unreachable."""
        port = await self._resolve_port()
        if port is None:
            return None
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(f"http://127.0.0.1:{port}/avatar/change")
                resp.raise_for_status()
                node = resp.json()
            val = node.get("VALUE")
            if isinstance(val, list) and val:
                val = val[0]
            return str(val) if isinstance(val, str) else None
        except Exception as exc:
            self._log.debug("oscquery.get_avatar_id_failed", extra={"error": str(exc)})
            return None

    async def get_avatar_parameters(self) -> list[OscQueryParameter]:
        """Return full /avatar/parameters tree with live values."""
        port = await self._resolve_port()
        if port is None:
            return []
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"http://127.0.0.1:{port}/")
                resp.raise_for_status()
                root = resp.json()
            params_node = (
                root.get("CONTENTS", {})
                    .get("avatar", {})
                    .get("CONTENTS", {})
                    .get("parameters")
            )
            if not params_node:
                return []
            results: list[OscQueryParameter] = []
            _walk(params_node, results)
            return results
        except Exception as exc:
            self._log.debug("oscquery.get_params_failed", extra={"error": str(exc)})
            return []

    async def get_parameter(self, name: str) -> OscQueryParameter | None:
        """Return a single parameter by short name (e.g. 'IsLocal'), or None."""
        params = await self.get_avatar_parameters()
        suffix = f"/{name}"
        for p in params:
            if p.path.endswith(suffix):
                return p
        return None

    # ── Discovery ────────────────────────────────────────────────────────────

    async def _resolve_port(self) -> int | None:
        if self._port is not None:
            return self._port

        port = await self._find_via_process()
        if port is not None:
            self._port = port
            self._mdns_last_failed_at = None
            return port

        # Avoid spinning up a zeroconf browse on every single call while
        # VRChat isn't running (e.g. repeated status polls) — that's exactly
        # the kind of always-on background cost we're trying to avoid.
        if (
            self._mdns_last_failed_at is not None
            and (time.monotonic() - self._mdns_last_failed_at) < self._mdns_negative_cache_s
        ):
            return None

        port = await self._wait_mdns(timeout=self._mdns_timeout_s)
        if port is not None:
            self._port = port
            self._mdns_last_failed_at = None
        else:
            self._mdns_last_failed_at = time.monotonic()
        return port

    async def _find_via_process(self) -> int | None:
        """psutil-based fast path: find VRChat PID → listening ports → probe."""
        try:
            import psutil
        except ImportError:
            return None

        try:
            vrchat_pid: int | None = None
            for proc in psutil.process_iter(["name", "pid"]):
                if (proc.info.get("name") or "").lower() == "vrchat.exe":
                    vrchat_pid = proc.info["pid"]
                    break

            if vrchat_pid is None:
                return None

            try:
                proc = psutil.Process(vrchat_pid)
                conns = proc.net_connections("tcp")
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                conns = [c for c in psutil.net_connections("tcp") if c.pid == vrchat_pid]

            for conn in conns:
                if conn.status == "LISTEN" and conn.laddr.port > 1024:
                    if await self._is_vrchat("127.0.0.1", conn.laddr.port):
                        self._log.info(
                            "oscquery.process_found",
                            extra={"pid": vrchat_pid, "port": conn.laddr.port},
                        )
                        return conn.laddr.port
        except Exception as exc:
            self._log.debug("oscquery.process_lookup_failed", extra={"error": str(exc)})
        return None

    async def _wait_mdns(self, timeout: float) -> int | None:
        """Briefly browse mDNS for VRChat's _oscjson._tcp announcement.

        The zeroconf listener (and its background thread) only lives for the
        duration of this call, bounded by `timeout` — it is not kept running
        between calls.
        """
        try:
            from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser
            from zeroconf import ServiceStateChange
        except ImportError:
            self._log.warning("oscquery.zeroconf_unavailable: mDNS fallback disabled (pip install zeroconf)")
            return None

        found: asyncio.Queue[int] = asyncio.Queue()

        async def _handler(zeroconf, service_type, name, state_change):
            if state_change is ServiceStateChange.Added:
                info = await zeroconf.async_get_service_info(service_type, name)
                if info and info.parsed_addresses():
                    port = info.port
                    addr = info.parsed_addresses()[0]
                    if await self._is_vrchat(addr, port):
                        self._log.info("oscquery.mdns_found", extra={"addr": addr, "port": port})
                        await found.put(port)

        zc = AsyncZeroconf()
        browser = None
        try:
            browser = AsyncServiceBrowser(zc.zeroconf, ["_oscjson._tcp.local."], handlers=[_handler])
            return await asyncio.wait_for(found.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        except Exception as exc:
            self._log.warning("oscquery.mdns_browse_failed", extra={"error": str(exc)})
            return None
        finally:
            try:
                if browser is not None:
                    await browser.async_cancel()
                await zc.async_close()
            except Exception:
                pass

    async def _is_vrchat(self, addr: str, port: int) -> bool:
        """Return True if the HTTP server at addr:port is VRChat OSCQuery."""
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                resp = await client.get(f"http://{addr}:{port}/avatar/parameters")
                node = resp.json()
                return node.get("FULL_PATH") == "/avatar/parameters"
        except Exception:
            return False


# ── Internal helpers ─────────────────────────────────────────────────────────

def _walk(node: dict[str, Any], results: list[OscQueryParameter]) -> None:
    if "TYPE" in node and "FULL_PATH" in node:
        val = node.get("VALUE")
        if isinstance(val, list) and val:
            val = val[0]
        results.append(
            OscQueryParameter(
                path=node["FULL_PATH"],
                type=node["TYPE"],
                value=val,
                access=node.get("ACCESS", 0),
            )
        )
    for child in (node.get("CONTENTS") or {}).values():
        _walk(child, results)
