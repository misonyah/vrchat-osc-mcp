from __future__ import annotations

import asyncio

import pytest

from vrchat_osc_mcp import main as main_module
from vrchat_osc_mcp.mcp_server import ActivityTracker


class _NullLogger:
    def warning(self, *args, **kwargs) -> None:
        return


@pytest.mark.asyncio
async def test_watchdog_returns_once_idle_past_timeout():
    activity = ActivityTracker()

    await asyncio.wait_for(
        main_module._mcp_idle_watchdog(
            activity=activity,
            idle_timeout_s=0.05,
            poll_interval_s=0.02,
            logger=_NullLogger(),
        ),
        timeout=1.0,
    )


@pytest.mark.asyncio
async def test_watchdog_resets_timer_while_activity_continues():
    activity = ActivityTracker()

    task = asyncio.create_task(
        main_module._mcp_idle_watchdog(
            activity=activity,
            idle_timeout_s=0.05,
            poll_interval_s=0.02,
            logger=_NullLogger(),
        )
    )

    # Keep touching activity faster than the idle timeout; watchdog must not fire.
    for _ in range(8):
        await asyncio.sleep(0.02)
        activity.touch()
    assert not task.done()

    # Stop touching; watchdog should now fire once idle_timeout_s elapses.
    await asyncio.wait_for(task, timeout=1.0)
