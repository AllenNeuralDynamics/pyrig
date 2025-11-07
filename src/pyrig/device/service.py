import asyncio
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Self

import structlog
import zmq
import zmq.asyncio
from pydantic import ValidationError

from pyrig.device.base import (
    _GET_CMD_,
    _INT_CMD_,
    _REQ_CMD_,
    _SET_CMD_,
    LABEL_ATTR,
    AttributeRequest,
    Command,
    CommandResponse,
    Device,
    DeviceInterface,
    ErrorMsg,
    PropertyInfo,
    PropertyState,
    PropsResponse,
)
from pyrig.device.conn import DeviceAddress, DeviceAddressTCP


class DeviceService[D: Device]:
    def __init__(self, device: D, conn: DeviceAddress, zctx: zmq.asyncio.Context):
        self.log = structlog.get_logger(device=f"{device.uid}.{self.__class__.__name__}")
        self._device = device
        self._client_conn = conn
        self._conn: DeviceAddress = conn.as_open() if isinstance(conn, DeviceAddressTCP) else conn

        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="DeviceService")
        self._commands = self._collect_commands()
        self._interface = DeviceInterface(
            uid=self._device.uid,
            type=self._device.__DEVICE_TYPE__,
            commands={name: cmd.info for name, cmd in self._commands.items()},
            properties=self._collect_properties(),
        )

        self._rep_socket = zctx.socket(zmq.REP)
        self._pub_socket = zctx.socket(zmq.PUB)

        self._rep_socket.bind(self._conn.rpc_addr)
        self._pub_socket.bind(self._conn.pub_addr)

        self._loop_tasks: dict[str, asyncio.Task] = {
            "cmd": asyncio.create_task(self._cmd_loop()),
            "state": asyncio.create_task(self._state_stream_loop(interval=0.5)),
            "heartbeat": asyncio.create_task(self._heartbeat_loop(interval=2.0)),
        }

    @property
    def device(self) -> D:
        return self._device

    @property
    def client_conn(self) -> DeviceAddress:
        return self._client_conn

    async def _exec[R](self, fn: Callable[..., R]) -> R:
        return await asyncio.get_event_loop().run_in_executor(self._executor, fn)

    async def _get_props(self, props: Sequence[str] | None = None) -> PropsResponse:
        props_to_get = props or list(self._interface.properties.keys())

        def _get_props_sync() -> PropsResponse:
            res: dict[str, PropertyState] = {}
            err: dict[str, ErrorMsg] = {}
            for prop_name in props_to_get:
                try:
                    val = getattr(self._device, prop_name)
                    res[prop_name] = PropertyState(value=val)
                except Exception as e:
                    err[prop_name] = ErrorMsg(msg=str(e))
            return PropsResponse(res=res, err=err)

        return await self._exec(_get_props_sync)

    async def _set_props(self, props: dict[str, Any]) -> PropsResponse:
        def _set_props_sync() -> PropsResponse:
            res: dict[str, PropertyState] = {}
            err: dict[str, ErrorMsg] = {}
            for prop_name, prop_value in props.items():
                try:
                    setattr(self._device, prop_name, prop_value)
                    res[prop_name] = PropertyState(value=prop_value)
                except Exception as e:
                    err[prop_name] = ErrorMsg(msg=str(e))
            return PropsResponse(res=res, err=err)

        return await self._exec(_set_props_sync)

    async def _handle_req(self, req: AttributeRequest) -> CommandResponse:
        if (command := self._commands.get(req.attr)) is not None:
            if command.is_async:
                out = await command(*req.args, **req.kwargs)
            else:
                out = await self._exec(lambda: command(*req.args, **req.kwargs))
            return CommandResponse(res=out)
        else:
            return CommandResponse(res=ErrorMsg(msg=f"Unknown command: {req.attr}"))

    async def _cmd_loop(self) -> None:
        """Listens for, decodes, executes, and replies to commands."""
        while True:
            res: CommandResponse | PropsResponse | None = None
            try:
                # topics are "REQ", "GET", "SET", "INT"
                topic, payload_bytes = await self._rep_socket.recv_multipart()
                try:
                    req = AttributeRequest.model_validate_json(payload_bytes)
                    if topic == _INT_CMD_:
                        res = CommandResponse(res=self._interface)
                    if topic == _GET_CMD_:
                        props = [p for p in (list(req.args) + list(req.kwargs.keys())) if isinstance(p, str)]
                        res = await self._get_props(props)
                    elif topic == _SET_CMD_:
                        res = await self._set_props(req.kwargs)
                    elif topic == _REQ_CMD_:
                        res = await self._handle_req(req)
                except ValidationError as e:
                    res = CommandResponse(res=ErrorMsg(msg=f"Invalid command payload: {e}"))
            except asyncio.CancelledError:
                break
            except Exception as e:
                res = CommandResponse(res=ErrorMsg(msg=f"Command execution failed: {e}"))
            finally:
                if res is not None:
                    await self._rep_socket.send_json(res.model_dump())

    async def _state_stream_loop(self, interval: float):
        """Periodically gets device state and publishes changes via the PUB socket."""
        last_state: PropsResponse | None = None

        while True:
            try:
                current_state = await self._get_props()
                for name, value in current_state.res.items():
                    last_value = last_state.res.get(name) if last_state else None
                    if last_state is None or last_value is None or value != last_value:
                        topic = f"{self._device.uid}/state/{name}".encode("utf-8")
                        payload = value.model_dump_json().encode("utf-8")
                        await self._pub_socket.send_multipart([topic, payload])
                last_state = current_state
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

    async def _heartbeat_loop(self, interval: float):
        """Continuously publish heartbeat messages."""
        while True:
            try:
                topic = f"{self._device.uid}/heartbeat".encode()
                payload = str(time.time()).encode()
                await self._pub_socket.send_multipart([topic, payload])

                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Heartbeat error: {e}")

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup."""

    def _collect_properties(self) -> dict[str, PropertyInfo]:
        properties: dict[str, PropertyInfo] = {}
        for attr_name in dir(self._device):
            if attr_name.startswith("_"):
                continue

            # Get the attribute from the class (where property descriptors live)
            try:
                attr = getattr(type(self._device), attr_name, None)
                if attr is None:
                    continue

                if isinstance(attr, property) and attr.fget is not None:
                    properties[attr_name] = PropertyInfo.from_attr(attr)
            except Exception as e:
                self.log.error(f"Error accessing attribute '{attr_name}': {e}")

        return properties

    def _collect_commands(self) -> dict[str, Command[Any]]:
        commands: dict[str, Command] = {}

        # Collect from node.COMMANDS
        for attr_name in self._device.__COMMANDS__:
            if hasattr(self._device, attr_name):
                attr = getattr(self._device, attr_name)
                if callable(attr):
                    commands[attr_name] = Command(attr)

        # Collect @describe decorated methods
        for attr_name in dir(self._device):
            if not attr_name.startswith("_"):
                attr = getattr(self._device, attr_name)
                if callable(attr) and hasattr(attr, LABEL_ATTR):
                    commands[attr_name] = Command(attr)

        # Collect @describe decorated agent methods
        for attr_name in dir(self):
            if not attr_name.startswith("_"):
                attr = getattr(self, attr_name)
                if callable(attr) and hasattr(attr, LABEL_ATTR):
                    commands[attr_name] = Command(attr)

        return commands

    def close(self):
        self._cleanup()

    def _cleanup(self):
        if hasattr(self, "_loop_tasks"):
            [task.cancel() for task in self._loop_tasks.values()]

        self._rep_socket.close()
        self._pub_socket.close()
        self._executor.shutdown(wait=True, cancel_futures=True)

    def __del__(self):
        self._cleanup()
