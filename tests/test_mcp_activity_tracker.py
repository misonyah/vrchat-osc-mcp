from __future__ import annotations

import pytest

from vrchat_osc_mcp.mcp_server import ActivityTracker, _ActivityMiddleware


@pytest.mark.asyncio
async def test_activity_middleware_touches_tracker_on_message():
    tracker = ActivityTracker()
    tracker.last_activity_monotonic = 0.0  # force a known "stale" baseline

    middleware = _ActivityMiddleware(tracker)

    async def _call_next(context):
        return "ok"

    result = await middleware.on_message(context=object(), call_next=_call_next)

    assert result == "ok"
    assert tracker.last_activity_monotonic > 0.0
