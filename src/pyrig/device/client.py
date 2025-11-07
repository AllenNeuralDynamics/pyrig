import asyncio
import time
from collections.abc import Callable
from typing import Any

import structlog
import zmq
import zmq.asyncio

from pyrig.device.base import (
    _GET_CMD_,
    _INT_CMD_,
    _REQ_CMD_,
    _SET_CMD_,
    AttributeRequest,
    CommandResponse,
    DeviceInterface,
    PropsResponse,
)
from pyrig.device.conn import DeviceAddress


def set_tcp_keepalive(socket: zmq.asyncio.Socket):
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 1)  # Start after 1s
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)  # Probe every 1s
    socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 3)  # 3 failed probes = dead
    return socket


class DeviceClient:
    def __init__(self, uid: str, zctx: zmq.asyncio.Context, conn: DeviceAddress):
        self.log = structlog.get_logger(device=f"{uid}.{self.__class__.__name__}")
        self._uid = uid
        self._last_heartbeat_time = 0.0

        self._req_socket = set_tcp_keepalive(zctx.socket(zmq.REQ))
        self._sub_socket = set_tcp_keepalive(zctx.socket(zmq.SUB))

        # Set timeouts
        self._req_socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5s timeout
        self._req_socket.setsockopt(zmq.SNDTIMEO, 5000)

        self._req_socket.connect(conn.rpc_addr)
        self._sub_socket.connect(conn.pub_addr)
        self._sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        self._listen_task = asyncio.create_task(self._listen_loop())
        self._callbacks: dict[str, list[Callable]] = {}

        asyncio.create_task(self._subscribe_to_heartbeat())

    @property
    def uid(self):
        return self._uid

    @property
    def is_connected(self) -> bool:
        """Check if device is connected (received heartbeat in last 10 seconds)."""
        if self._last_heartbeat_time == 0:
            return False
        time_since_last = time.time() - self._last_heartbeat_time
        return time_since_last < 10.0

    async def wait_for_connection(self, timeout: float = 10.0) -> bool:
        """Wait for node to connect."""
        start = time.time()
        while time.time() - start < timeout:
            if self.is_connected:
                return True
            await asyncio.sleep(0.1)
        return False

    def close(self):
        """Closes the socket."""
        if self._listen_task:
            self._listen_task.cancel()

        if self._sub_socket:
            self._sub_socket.close()

        if self._req_socket:
            self._req_socket.close()

    async def subscribe(self, topic: str, callback: Callable[[bytes], None]):
        if not topic.startswith(self.uid):
            topic = f"{self.uid}/{topic}"
        if topic not in self._callbacks:
            self._callbacks[topic] = []
        self._callbacks[topic].append(callback)

    async def call(self, command: str, *args: Any, **kwargs: Any):
        """Call a command and unwrap the result, raising on error."""
        response = await self.send_command(command, *args, **kwargs)
        return response.unwrap()

    async def request(self, req: AttributeRequest) -> CommandResponse:
        """Sends a command to a specific device."""
        payload = req.model_dump_json().encode()

        await self._req_socket.send_multipart([_REQ_CMD_, payload])
        response_json = await self._req_socket.recv_json()
        return CommandResponse.model_validate(response_json)

    async def send_command(self, command: str, *args: Any, **kwargs: Any) -> CommandResponse:
        """Send a command to the device and return raw response (for manual error handling)."""
        req = AttributeRequest(node=self.uid, attr=command, args=list(args), kwargs=kwargs)
        return await self.request(req)

    async def get_prop(self, prop_name: str) -> Any:
        """Get a single property value, raising on error."""
        props = await self.get_props(prop_name)
        if prop_name in props.err:
            raise RuntimeError(f"Failed to get {prop_name}: {props.err[prop_name].msg}")
        return props.res[prop_name].value

    async def set_prop(self, prop_name: str, value: Any) -> None:
        """Set a single property value, raising on error."""
        props = await self.set_props(**{prop_name: value})
        if prop_name in props.err:
            raise RuntimeError(f"Failed to set {prop_name}: {props.err[prop_name].msg}")

    async def get_props(self, *props: str) -> PropsResponse:
        """Get property values from the device."""
        req = AttributeRequest(node=self.uid, attr="", args=list(props))
        payload = req.model_dump_json().encode()
        await self._req_socket.send_multipart([_GET_CMD_, payload])
        response_json = await self._req_socket.recv_json()
        return PropsResponse.model_validate(response_json)

    async def set_props(self, **props: Any) -> PropsResponse:
        """Set property values on the device."""
        req = AttributeRequest(node=self.uid, attr="", kwargs=props)
        payload = req.model_dump_json().encode()
        await self._req_socket.send_multipart([_SET_CMD_, payload])
        response_json = await self._req_socket.recv_json()
        return PropsResponse.model_validate(response_json)

    async def get_interface(self) -> DeviceInterface:
        """Get the device interface information."""
        req = AttributeRequest(node=self.uid, attr="")
        payload = req.model_dump_json().encode()
        await self._req_socket.send_multipart([_INT_CMD_, payload])
        response_json = await self._req_socket.recv_json()
        cmd_response: CommandResponse[dict] = CommandResponse.model_validate(response_json)
        interface_dict = cmd_response.unwrap()
        return DeviceInterface.model_validate(interface_dict)

    async def _listen_loop(self):
        """The background loop that receives and dispatches state updates."""
        while True:
            try:
                topic_bytes, payload_bytes = await self._sub_socket.recv_multipart()
                topic = topic_bytes.decode()
                for callback in self._callbacks.get(topic, []):
                    callback(payload_bytes)
            except asyncio.CancelledError:
                break

    async def _subscribe_to_heartbeat(self):
        """Subscribe to heartbeat messages."""

        def handle_heartbeat(payload_bytes: bytes):
            try:
                # Update with local receive time (could parse payload_bytes for server time if needed later)
                self._last_heartbeat_time = time.time()
            except Exception as e:
                self.log.error(f"Error handling heartbeat: {e}")

        await self.subscribe("heartbeat", handle_heartbeat)
