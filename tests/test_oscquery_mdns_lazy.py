from __future__ import annotations

import asyncio

import pytest

from vrchat_osc_mcp.oscquery.client import OscQueryClient


@pytest.mark.asyncio
async def test_repeated_resolve_does_not_rebrowse_within_negative_cache_window():
    """Regression test: previously mDNS browsing ran continuously for the
    whole process lifetime. Now a failed discovery should be cached for
    mdns_negative_cache_s so status polls while VRChat is closed don't keep
    spinning up a new zeroconf browse.
    """
    client = OscQueryClient(mdns_negative_cache_s=10.0)

    async def _no_process() -> None:
        return None

    calls = 0

    async def _fake_wait_mdns(timeout: float):
        nonlocal calls
        calls += 1
        return None

    client._find_via_process = _no_process  # type: ignore[method-assign]
    client._wait_mdns = _fake_wait_mdns  # type: ignore[method-assign]

    assert await client._resolve_port() is None
    assert await client._resolve_port() is None
    assert await client._resolve_port() is None

    assert calls == 1


@pytest.mark.asyncio
async def test_resolve_rebrowses_after_negative_cache_expires():
    client = OscQueryClient(mdns_negative_cache_s=0.05)

    async def _no_process() -> None:
        return None

    calls = 0

    async def _fake_wait_mdns(timeout: float):
        nonlocal calls
        calls += 1
        return None

    client._find_via_process = _no_process  # type: ignore[method-assign]
    client._wait_mdns = _fake_wait_mdns  # type: ignore[method-assign]

    assert await client._resolve_port() is None
    await asyncio.sleep(0.1)
    assert await client._resolve_port() is None

    assert calls == 2


@pytest.mark.asyncio
async def test_invalidate_clears_negative_cache():
    client = OscQueryClient(mdns_negative_cache_s=10.0)

    async def _no_process() -> None:
        return None

    calls = 0

    async def _fake_wait_mdns(timeout: float):
        nonlocal calls
        calls += 1
        return None

    client._find_via_process = _no_process  # type: ignore[method-assign]
    client._wait_mdns = _fake_wait_mdns  # type: ignore[method-assign]

    assert await client._resolve_port() is None
    client.invalidate()
    assert await client._resolve_port() is None

    assert calls == 2
