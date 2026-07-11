from __future__ import annotations

import uuid

import pytest

from vrchat_osc_mcp.singleton import InstanceAlreadyRunningError, SingleInstanceLock


def test_second_acquire_fails_while_first_holds_lock():
    name = f"vrchat-osc-mcp-test-{uuid.uuid4().hex}"
    first = SingleInstanceLock(name)
    second = SingleInstanceLock(name)

    first.acquire()
    try:
        with pytest.raises(InstanceAlreadyRunningError):
            second.acquire()
    finally:
        first.release()


def test_acquire_succeeds_again_after_release():
    name = f"vrchat-osc-mcp-test-{uuid.uuid4().hex}"
    lock = SingleInstanceLock(name)

    lock.acquire()
    lock.release()

    # Should not raise: the lock was released, so a fresh acquire succeeds.
    other = SingleInstanceLock(name)
    other.acquire()
    other.release()
