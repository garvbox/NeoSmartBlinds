import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest
from custom_components.neosmartblinds.const import DATA_NEOSMARTBLINDS
from custom_components.neosmartblinds.cover import NeoSmartBlindsCover


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture
async def hass_mock():
    """Fixture to mock Home Assistant instance and clean up client sessions."""
    hass = MagicMock()
    tasks = []
    
    def mock_async_create_task(coro):
        task = asyncio.create_task(coro)
        tasks.append(task)
        return task
        
    hass.async_create_task = mock_async_create_task
    hass.data = {}
    
    yield hass, tasks
    
    if DATA_NEOSMARTBLINDS in hass.data:
        await hass.data[DATA_NEOSMARTBLINDS].close()


@pytest.fixture
def cover_factory(hass_mock):
    """Fixture factory to instantiate and configure covers for testing."""
    hass, _ = hass_mock
    
    def _create_cover(name, parent_code=""):
        cover = NeoSmartBlindsCover(
            home_assistant=hass,
            name=name,
            host="127.0.0.1",
            the_id="hub1",
            device=name,
            close_time=2,  # Must be an integer to avoid being truncated to 0 by int()
            protocol="http",
            port=8838,
            rail=1,
            percent_support=2,
            motor_code="",
            starting_position=100,
            parent_code=parent_code,
            tilt_enabled=False,
        )
        cover.async_write_ha_state = MagicMock()
        cover.hass = hass
        return cover
        
    return _create_cover


@pytest.mark.anyio
async def test_async_close_cover_to_keeps_correct_timing_when_slow_response_from_hub(
    hass_mock, cover_factory
):
    """
    Test that the cover stops at the correct time even when the hub's
    start command response is slow.
    """
    hass, tasks = hass_mock
    cover = cover_factory("test_blind")

    stop_command_called_time = None
    
    async def mock_async_down_command(*args, **kwargs):
        await asyncio.sleep(0.05)
        return True

    async def mock_async_stop_command(*args, **kwargs):
        nonlocal stop_command_called_time
        stop_command_called_time = time.perf_counter()
        return True

    cover._client.async_down_command = mock_async_down_command
    cover._client.async_stop_command = mock_async_stop_command

    start_time = time.perf_counter()
    
    # Trigger closing the cover from 100 to 50.
    # Close time is 2s, moving 50% should take 1.0 seconds.
    await cover.async_close_cover_to(50)
    
    # Wait for the background task to complete
    assert len(tasks) == 1
    await tasks[0]
    
    assert stop_command_called_time is not None
    elapsed = stop_command_called_time - start_time
    
    # Ideally elapsed should be around 1.0 seconds.
    # With the bug, elapsed will be 0.05 (response delay) + 1.0 (wait time) = 1.05 seconds.
    # We assert that elapsed is close to 1.0 seconds (e.g., < 1.02 seconds).
    assert elapsed == pytest.approx(1.0, abs=0.02), (
        f"Stop command was delayed, elapsed time was {elapsed:.3f} seconds "
        "instead of ~1.0 seconds"
    )


@pytest.mark.anyio
async def test_async_close_cover_to_keeps_correct_timing_when_aggregated_group_command(
    hass_mock, cover_factory
):
    """
    Test that group command aggregation correctly registers and resets timing for all child blinds.
    """
    # Clear any leftover parents global state
    from custom_components.neosmartblinds.neo_smart_blind import parents
    parents.clear()

    hass, tasks = hass_mock
    cover1 = cover_factory("blind1", "group1")
    cover2 = cover_factory("blind2", "group1")

    stop_command_called_times = {}

    async def mock_async_send_command_to_device(command, device):
        # Simulate network response latency of 0.05s
        await asyncio.sleep(0.05)
        return True

    async def mock_async_stop_command_1(*args, **kwargs):
        stop_command_called_times["blind1"] = time.perf_counter()
        return True

    async def mock_async_stop_command_2(*args, **kwargs):
        stop_command_called_times["blind2"] = time.perf_counter()
        return True

    cover1._client._command_sender.async_send_command_to_device = mock_async_send_command_to_device
    cover2._client._command_sender.async_send_command_to_device = mock_async_send_command_to_device
    cover1._client.async_stop_command = mock_async_stop_command_1
    cover2._client.async_stop_command = mock_async_stop_command_2

    # Patch the constants to make the test run very quickly
    nsb_path = "custom_components.neosmartblinds.neo_smart_blind"
    with patch(f"{nsb_path}.DEFAULT_COMMAND_AGGREGATION_PERIOD", 0.02), \
         patch(f"{nsb_path}.DEFAULT_COMMAND_BACKOFF", 0.05):
        
        start_time = time.perf_counter()

        # Trigger both close commands concurrently
        await asyncio.gather(
            cover1.async_close_cover_to(50),
            cover2.async_close_cover_to(50),
        )

        # Wait for the background tasks (which wait for the movements to finish)
        assert len(tasks) == 2
        await asyncio.gather(*tasks)

        # Both stop commands should be called
        assert "blind1" in stop_command_called_times
        assert "blind2" in stop_command_called_times

        elapsed1 = stop_command_called_times["blind1"] - start_time
        elapsed2 = stop_command_called_times["blind2"] - start_time

        # The group aggregation period is 0.02s.
        # The start command network response latency is 0.05s.
        # The move time is 1.0s.
        # The start command is sent after the 0.02s aggregation delay.
        # So the group command is sent at t = 0.02s, and returns at t = 0.07s.
        # With correct timing, both covers should stop exactly 1.0 seconds after the
        # group command was sent. So they should stop at t = 0.02 + 1.0 = 1.02s.
        # If the bug was present, elapsed would be aggregation delay (0.02s) +
        # response delay (0.05s) + 1.0s = 1.07s.
        # Let's assert that both elapsed times are approximately 1.02s.
        assert elapsed1 == pytest.approx(1.02, abs=0.02), (
            f"Blind 1 stop command was delayed: {elapsed1:.3f}s"
        )
        assert elapsed2 == pytest.approx(1.02, abs=0.02), (
            f"Blind 2 stop command was delayed: {elapsed2:.3f}s"
        )


@pytest.mark.anyio
async def test_async_close_cover_to_keeps_correct_timing_when_independent_group_command(
    hass_mock, cover_factory
):
    """
    Test that when covers do not share a parent group, their commands are spaced
    by the backoff delay, and each cover's timer is reset individually, ensuring
    correct independent timing.
    """
    hass, tasks = hass_mock
    # Create two covers with NO parent group
    cover1 = cover_factory("blind1", parent_code="")
    cover2 = cover_factory("blind2", parent_code="")

    stop_command_called_times = {}

    async def mock_async_send_command_to_device(command, device):
        # Simulate network response latency of 0.05s
        await asyncio.sleep(0.05)
        return True

    async def mock_async_stop_command_1(*args, **kwargs):
        stop_command_called_times["blind1"] = time.perf_counter()
        return True

    async def mock_async_stop_command_2(*args, **kwargs):
        stop_command_called_times["blind2"] = time.perf_counter()
        return True

    cover1._client._command_sender.async_send_command_to_device = mock_async_send_command_to_device
    cover2._client._command_sender.async_send_command_to_device = mock_async_send_command_to_device
    cover1._client.async_stop_command = mock_async_stop_command_1
    cover2._client.async_stop_command = mock_async_stop_command_2

    # Patch backoff to 0.05s
    nsb_path = "custom_components.neosmartblinds.neo_smart_blind"
    with patch(f"{nsb_path}.DEFAULT_COMMAND_BACKOFF", 0.05):
        start_time = time.perf_counter()

        await asyncio.gather(
            cover1.async_close_cover_to(50),
            cover2.async_close_cover_to(50),
        )

        assert len(tasks) == 2
        await asyncio.gather(*tasks)

        assert "blind1" in stop_command_called_times
        assert "blind2" in stop_command_called_times

        elapsed1 = stop_command_called_times["blind1"] - start_time
        elapsed2 = stop_command_called_times["blind2"] - start_time

        # Let's trace the timing (up lock serializes commands in gather):
        # Cover 1:
        # - Acquires _up_lock at t = 0.
        # - Backoff: global time_of_last_command is 0 → no sleep.
        # - Sets start timer for cover 1 at t = 0.
        # - Sends command to device 1 (takes 0.05s to return).
        # - time_of_last_command = 0.05, lock released at t = 0.05.
        # - cover 1 wait starts. Timer _start = 0, elapsed = 0.05, so wait 0.95s.
        # - cover 1 stops at t = 1.0s.
        # Cover 2:
        # - Acquires _up_lock at t = 0.05 (was waiting).
        # - Backoff: since_last = 0.05 - 0.05 = 0 → sleep 0.05s.
        # - Sets start timer for cover 2 at t = 0.10.
        # - Sends command to device 2 (takes 0.05s to return).
        # - cover 2 wait starts. Timer _start = 0.10.
        # - cover 2 stops at ~1.10s.
        assert elapsed1 == pytest.approx(1.0, abs=0.02), (
            f"Blind 1 stop command timing incorrect: {elapsed1:.3f}s"
        )
        assert elapsed2 == pytest.approx(1.10, abs=0.02), (
            f"Blind 2 stop command timing incorrect: {elapsed2:.3f}s"
        )


@pytest.mark.anyio
async def test_async_close_cover_to_keeps_correct_timing_when_sequential_same_parent(
    hass_mock, cover_factory
):
    """
    Test that when covers share a parent_group but are processed sequentially
    (outside the aggregation window), each cover gets its own individual
    command with correct independent timing.
    """
    from custom_components.neosmartblinds.neo_smart_blind import parents
    parents.clear()

    hass, tasks = hass_mock
    cover1 = cover_factory("blind1", "group1")
    cover2 = cover_factory("blind2", "group1")

    stop_command_called_times = {}

    async def mock_async_send_command_to_device(command, device):
        await asyncio.sleep(0.05)
        return True

    async def mock_async_stop_command_1(*args, **kwargs):
        stop_command_called_times["blind1"] = time.perf_counter()
        return True

    async def mock_async_stop_command_2(*args, **kwargs):
        stop_command_called_times["blind2"] = time.perf_counter()
        return True

    cover1._client._command_sender.async_send_command_to_device = mock_async_send_command_to_device
    cover2._client._command_sender.async_send_command_to_device = mock_async_send_command_to_device
    cover1._client.async_stop_command = mock_async_stop_command_1
    cover2._client.async_stop_command = mock_async_stop_command_2

    nsb_path = "custom_components.neosmartblinds.neo_smart_blind"
    with patch(f"{nsb_path}.DEFAULT_COMMAND_AGGREGATION_PERIOD", 0.02), \
         patch(f"{nsb_path}.DEFAULT_COMMAND_BACKOFF", 0.05):

        start_time = time.perf_counter()

        # Process cover1 and let its movement complete before starting cover2
        await cover1.async_close_cover_to(50)
        assert len(tasks) == 1
        await tasks[0]

        # Now process cover2 (cover1's parent state has been fully cleaned up)
        await cover2.async_close_cover_to(50)
        assert len(tasks) == 2
        await tasks[1]

        assert "blind1" in stop_command_called_times
        assert "blind2" in stop_command_called_times

        elapsed1 = stop_command_called_times["blind1"] - start_time
        elapsed2 = stop_command_called_times["blind2"] - start_time

        # Cover 1:
        # - t=0: start
        # - t=0~0.02: parent aggregation backoff (no other child registered)
        # - t=0.02: start_timer fires, command sent (0.05s network latency)
        # - t=0.07: command returns, background wait task starts
        # - Movement: 1.0s from t=0.02
        # - Stop at ~1.02s
        # Cover 2 (starts after cover1's movement fully completes):
        # - t=1.02: start
        # - t=1.02~1.04: parent aggregation backoff (fresh parent state)
        # - t=1.04: start_timer fires, command sent
        # - t=1.09: command returns
        # - Movement: 1.0s from t=1.04
        # - Stop at ~2.04s
        #
        # Both must run individually for the full close duration without aggregation
        assert elapsed1 == pytest.approx(1.02, abs=0.04), (
            f"Blind 1 stop command timing incorrect: {elapsed1:.3f}s"
        )
        assert elapsed2 == pytest.approx(elapsed1 + 1.0, abs=0.06), (
            f"Blind 2 should NOT share blind 1's timer: "
            f"elapsed1={elapsed1:.3f}s, elapsed2={elapsed2:.3f}s"
        )
