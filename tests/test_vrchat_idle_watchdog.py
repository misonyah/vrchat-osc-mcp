from __future__ import annotations

import asyncio

import pytest

from vrchat_osc_mcp import main as main_module


class _NullLogger:
    def warning(self, *args, **kwargs) -> None:
        return


@pytest.mark.asyncio
async def test_watchdog_returns_once_vrchat_absent_past_timeout(monkeypatch):
    monkeypatch.setattr(main_module, "_vrchat_is_running", lambda: False)

    await asyncio.wait_for(
        main_module._vrchat_idle_watchdog(
            idle_timeout_s=0.05,
            poll_interval_s=0.02,
            logger=_NullLogger(),
        ),
        timeout=1.0,
    )


@pytest.mark.asyncio
async def test_watchdog_resets_timer_while_vrchat_running(monkeypatch):
    running = True
    monkeypatch.setattr(main_module, "_vrchat_is_running", lambda: running)

    task = asyncio.create_task(
        main_module._vrchat_idle_watchdog(
            idle_timeout_s=0.05,
            poll_interval_s=0.02,
            logger=_NullLogger(),
        )
    )

    # While VRChat is "running", the watchdog must not fire.
    await asyncio.sleep(0.15)
    assert not task.done()

    running = False
    await asyncio.wait_for(task, timeout=1.0)
