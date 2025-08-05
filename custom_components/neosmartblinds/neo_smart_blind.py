import asyncio
import logging
import time
from datetime import datetime

from .const import (
    CMD_DOWN,
    CMD_DOWN2,
    CMD_FAV,
    CMD_FAV_2,
    CMD_MICRO_DOWN,
    CMD_MICRO_DOWN2,
    CMD_MICRO_UP,
    CMD_MICRO_UP2,
    CMD_STOP,
    CMD_UP,
    CMD_UP2,
    DEFAULT_COMMAND_AGGREGATION_PERIOD,
    DEFAULT_COMMAND_BACKOFF,
    DEFAULT_IO_TIMEOUT,
)

logger = logging.getLogger(__name__)

parents = {}


class NeoParentBlind:
    def __init__(self):
        self._child_count = 0
        self._intent = 0
        self._fulfilled = False
        self._time_of_first_intent = None
        self._intended_command = None
        self._wait = asyncio.Event()

    def add_child(self):
        self._child_count += 1

    def register_intent(self, command):
        if self._intended_command is None:
            self._intended_command = command

        if self._intended_command != command:
            logger.debug(
                f"{command}, abandoning aggregated group command {self._intended_command} with "
                f"{self._intent} waiting"
            )
            self._wait.set()
            return False

        self._intent += 1
        if self._intent >= self._child_count:
            self._fulfilled = True
            self._wait.set()

        if self._time_of_first_intent is None:
            self._time_of_first_intent = time.time() + DEFAULT_COMMAND_AGGREGATION_PERIOD

        return True

    def unregister_intent(self):
        self._intent -= 1
        if self._intent == 0:
            self._fulfilled = False
            self._time_of_first_intent = None
            self._intended_command = None
            self._wait.clear()

    CHANGE_DEVICE = 0
    IGNORE = 1
    USE_DEVICE = 2

    def fulfilled(self):
        return self._fulfilled

    def act_on_intent(self):
        if not self._fulfilled:
            return NeoParentBlind.USE_DEVICE

        if self._intent == self._child_count:
            return NeoParentBlind.CHANGE_DEVICE

        return NeoParentBlind.IGNORE

    async def async_backoff(self):
        now = time.time()
        sleep_duration = (
            self._time_of_first_intent - now if self._time_of_first_intent is not None else 0
        )
        if sleep_duration > 0:
            logger.debug(
                f"{self._intended_command}, observing blind commands for {sleep_duration:.3f}s"
            )
            try:
                await asyncio.wait_for(asyncio.create_task(self._wait.wait()), sleep_duration)
            except TimeoutError:
                # all done
                pass


time_of_last_command = time.time()


async def async_backoff():
    global time_of_last_command
    now = time.time()
    sleep_duration = 0.0
    since_last = now - time_of_last_command

    if since_last < DEFAULT_COMMAND_BACKOFF:
        sleep_duration = DEFAULT_COMMAND_BACKOFF - since_last

    time_of_last_command = now + sleep_duration

    if sleep_duration > 0.0:
        logger.debug(f"Delaying command for {sleep_duration:.3f}s")
        await asyncio.sleep(sleep_duration)


class NeoCommandSender:
    def __init__(self, host, id, device, port, motor_code):
        self._host = host
        self._port = port
        self._id = id
        self._device = device
        self._motor_code = motor_code
        self._was_connected = None

    def on_io_complete(self, result=None):
        """
        Helper function to trap connection status and log it on change only.
        result is either None (success) or an exception (fail)
        """
        if result is None:
            if not self._was_connected:
                logger.info(f"{self._device}, connected to hub")
                self._was_connected = True
        else:
            if self._was_connected or self._was_connected is None:
                logger.warning(f"{self._device}, disconnected from hub: {repr(result)}")
                self._was_connected = False

        return self._was_connected

    @property
    def device(self):
        return self._device

    @property
    def motor_code(self):
        return self._motor_code

    def register_parents(self, device):
        global parents

        if device not in parents:
            parent = NeoParentBlind()
            parents[device] = parent

        parents[device].add_child()

    async def async_send_command(self, command, parent_device=None):
        global parents
        action = NeoParentBlind.USE_DEVICE

        if parent_device is not None:
            # find parent, register intent
            if parent_device in parents:
                logger.debug(f"{self._device}, checking for aggregation {parent_device}")
                parent = parents[parent_device]
                if parent.register_intent(command):
                    await parent.async_backoff()
                    action = parent.act_on_intent()
                    parent.unregister_intent()

        # if parent fulfilled, use it else continue
        if action == NeoParentBlind.USE_DEVICE:
            logger.debug(f"{self._device}, issuing command")
            await async_backoff()
            return await self.async_send_command_to_device(command, self._device)
        elif action == NeoParentBlind.CHANGE_DEVICE:
            logger.debug(f"{self._device}, issuing to group command instead {parent_device}")
            await async_backoff()
            return await self.async_send_command_to_device(command, parent_device)
        else:
            logger.debug(f"{self._device}, aggregated to group command")
            return True

    async def async_send_command_to_device(self, command, device):
        logger.error("I should do something but I cannot!")


class NeoTcpCommandSender(NeoCommandSender):
    async def async_send_command_to_device(self, command, device):
        """Command sender for TCP"""

        async def async_sender():
            """Wrap all the IO in an awaitable closure so a timeout can be put around it"""
            reader, writer = await asyncio.open_connection(self._host, self._port)

            mc = f"!{self._motor_code}" if self._motor_code else ""
            complete_command = device + "-" + command + mc + "\r\n"
            logger.debug(f"{device}, Tx: {complete_command}")
            writer.write(complete_command.encode())
            await writer.drain()

            response = await reader.read()
            logger.debug(f"{device}, Rx: {response.decode()}")

            writer.close()
            await writer.wait_closed()

        try:
            await asyncio.wait_for(async_sender(), timeout=DEFAULT_IO_TIMEOUT)
            return self.on_io_complete()
        except Exception as e:
            return self.on_io_complete(e)


class NeoHttpCommandSender(NeoCommandSender):
    def __init__(self, http_session_factory, host, the_id, device, port, motor_code):
        self._session = http_session_factory(DEFAULT_IO_TIMEOUT)
        super().__init__(host, the_id, device, port, motor_code)

    async def async_send_command_to_device(self, command, device):
        """Command sender for HTTP"""
        url = f"http://{self._host}:{self._port}/neo/v1/transmit"
        mc = f"!{self._motor_code}" if self._motor_code else ""
        hash_string = str(datetime.now().microsecond).zfill(7)

        params = {
            "id": self._id,
            "command": device + "-" + command + mc,
            "hash": hash_string,
        }

        try:
            async with self._session.get(url=url, params=params, raise_for_status=True) as r:
                logger.debug(f"{device}, Tx: {r.url}, Rx: {r.status} - {await r.text()}")
                return self.on_io_complete()
        except Exception as e:
            logger.error(f"Error: {e} URL: {url}, Parameters: {params}")
            return self.on_io_complete(e)


class NeoSmartBlind:
    _command_sender: NeoCommandSender | NeoHttpCommandSender

    def __init__(
        self,
        host,
        the_id,
        device,
        port,
        protocol,
        rail,
        motor_code,
        parent_code,
        http_session_factory,
    ):
        self._rail = rail
        self._parent_code = parent_code

        if self._rail < 1 or self._rail > 2:
            logger.error(f"{device}, unknown rail: {rail}, please use: 1 or 2")

        if protocol.lower() == "http":
            self._command_sender = NeoHttpCommandSender(
                http_session_factory, host, the_id, device, port, motor_code
            )
        elif protocol.lower() == "tcp":
            self._command_sender = NeoTcpCommandSender(host, the_id, device, port, motor_code)
        else:
            logger.error(f"{device}, unknown protocol: {protocol}, please use: http or tcp")
            return

        if parent_code is not None and len(parent_code) > 0:
            self._command_sender.register_parents(parent_code)

    def unique_id(self, prefix):
        return (
            f"{prefix}.{self._command_sender.device}.{self._command_sender.motor_code}.{self._rail}"
        )

    async def async_set_position_by_percent(self, pos):
        """Flip position percentage

        NeoBlinds works off of percent closed, but HA works off of percent open
        """
        closed_pos = 100 - pos
        padded_position = f"{closed_pos:02}"
        return await self._command_sender.async_send_command(padded_position, self._parent_code)

    async def async_stop_command(self):
        return await self._command_sender.async_send_command(CMD_STOP, self._parent_code)

    async def async_open_cover_tilt(self, **kwargs):
        if self._rail == 1:
            return await self._command_sender.async_send_command(CMD_MICRO_UP)
        elif self._rail == 2:
            return await self._command_sender.async_send_command(CMD_MICRO_UP2)
        return False

    async def async_close_cover_tilt(self, **kwargs):
        if self._rail == 1:
            return await self._command_sender.async_send_command(CMD_MICRO_DOWN)
        elif self._rail == 2:
            return await self._command_sender.async_send_command(CMD_MICRO_DOWN2)
        return False

    async def async_down_command(self):
        if self._rail == 1:
            return await self._command_sender.async_send_command(CMD_DOWN, self._parent_code)
        elif self._rail == 2:
            return await self._command_sender.async_send_command(CMD_DOWN2, self._parent_code)
        return False

    async def async_up_command(self):
        if self._rail == 1:
            return await self._command_sender.async_send_command(CMD_UP, self._parent_code)
        elif self._rail == 2:
            return await self._command_sender.async_send_command(CMD_UP2, self._parent_code)
        return False

    async def async_set_fav_position(self, pos):
        if pos <= 50:
            if await self._command_sender.async_send_command(CMD_FAV):
                return 50
        if pos >= 51:
            if await self._command_sender.async_send_command(CMD_FAV_2):
                return 51
        return False
