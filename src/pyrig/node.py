"""Node service for managing devices on local and remote hosts."""

import asyncio
import sys

import structlog
import zmq
import zmq.asyncio
from pydantic import BaseModel
from structlog.typing import EventDict, WrappedLogger

from pyrig.config import NodeConfig
from pyrig.device import Device, DeviceAddressTCP, DeviceService, DeviceType

logger = structlog.get_logger()


def _zmq_processor(zmq_socket: zmq.Socket):
    """Create a structlog processor that sends logs over ZMQ PUB socket.

    Args:
        zmq_socket: ZMQ PUB socket to send logs to

    Returns:
        Processor function for structlog
    """

    def processor(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
        """Send the log event as JSON over ZMQ."""
        try:
            # Send as JSON bytes
            zmq_socket.send_json(event_dict)
        except Exception:
            # Don't let logging errors break the application
            pass
        return event_dict

    return processor


def _get_node_logger(node_id: str, zmq_socket: zmq.Socket, level: str = "INFO") -> structlog.BoundLogger:
    """Configure structlog for a node with ZMQ transport and return logger.

    Args:
        node_id: Unique identifier for this node
        zmq_socket: ZMQ PUB socket connected to rig's log aggregator
        level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger bound with node_id context
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _zmq_processor(zmq_socket),  # Send over ZMQ
            structlog.processors.JSONRenderer(),  # Convert to JSON string for PrintLogger
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level.lower()),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=open("/dev/null", "w")),  # Discard local output
        cache_logger_on_first_use=False,
    )

    # Return logger with node_id bound
    return structlog.get_logger().bind(node_id=node_id)


class ProvisionResponse(BaseModel):
    """Payload: Controller sends configuration to node."""

    config: NodeConfig


class ProvisionedDevice(BaseModel):
    """Bundle of device connection and type information."""

    conn: DeviceAddressTCP
    device_type: DeviceType


class ProvisionComplete(BaseModel):
    """Payload: Node reports successful provisioning with device addresses."""

    devices: dict[str, ProvisionedDevice]


def build_devices(cfg: NodeConfig) -> dict[str, Device]:
    devices: dict[str, Device] = {}
    for uid, device_cfg in cfg.devices.items():
        cls = device_cfg.get_device_class()
        if uid not in device_cfg.kwargs:
            device_cfg.kwargs.update({"uid": uid})
        device = cls(**device_cfg.kwargs)
        devices[uid] = device
    return devices


class NodeService:
    """Service that manages devices on a node and communicates with the controller."""

    def __init__(
        self,
        zctx: zmq.asyncio.Context,
        node_id: str,
        ctrl_host: str,
        ctrl_port: int = 9000,
        log_port: int = 9001,
        start_port: int = 10000,
    ):
        """Initialize NodeService.

        Args:
            zctx: ZMQ context
            node_id: Unique identifier for this node
            ctrl_host: Controller hostname or IP address
            ctrl_port: Controller control port (default: 9000)
            log_port: Controller log aggregation port (default: 9001)
            start_port: Starting port for device allocation (default: 10000)
        """
        self._zctx = zctx
        self._node_id = node_id
        self._start_port = start_port

        # Connect to controller
        self._control_socket = self._zctx.socket(zmq.DEALER)
        self._control_socket.setsockopt(zmq.IDENTITY, node_id.encode())
        controller_addr = f"tcp://{ctrl_host}:{ctrl_port}"
        self._control_socket.connect(controller_addr)

        # Set up log socket
        log_addr = f"tcp://{ctrl_host}:{log_port}"
        self._log_socket = self._zctx.socket(zmq.PUB)
        self._log_socket.connect(log_addr)

        # Configure structured logging
        self.log = _get_node_logger(node_id, self._log_socket)

        self._device_servers: dict[str, DeviceService] = {}

    def _create_service(self, device: Device, conn) -> DeviceService:
        """Hook for custom service types."""
        return DeviceService(device, conn, self._zctx)

    async def run(self):
        """Run the node service - handle provisioning and control messages."""
        # Small delay to avoid ZMQ "slow joiner" issue where SUB socket misses early messages
        await asyncio.sleep(0.1)

        self.log.info("node_started")

        # Request config - no payload needed (identity tells controller who we are)
        await self._control_socket.send_multipart([b"", b"provision"])

        # Handle control messages
        try:
            while True:
                parts = await self._control_socket.recv_multipart()
                action = parts[1].decode()

                if action == "provision":
                    payload = parts[2]
                    response = ProvisionResponse.model_validate_json(payload)
                    node_cfg = response.config

                    try:
                        devices: dict[str, Device] = build_devices(node_cfg)
                        device_provs: dict[str, ProvisionedDevice] = {}
                        pc = self._start_port
                        for device_id, device in devices.items():
                            conn = DeviceAddressTCP(host=node_cfg.hostname, rpc=pc, pub=pc + 1)
                            self._device_servers[device_id] = self._create_service(device, conn)
                            device_provs[device_id] = ProvisionedDevice(conn=conn, device_type=device.__DEVICE_TYPE__)
                            pc += 2

                        # Report completion
                        complete = ProvisionComplete(devices=device_provs)
                        await self._control_socket.send_multipart(
                            [
                                b"",
                                b"provision_complete",
                                complete.model_dump_json().encode(),
                            ]
                        )

                    except Exception as e:
                        logger.error("provision_failed", error=str(e))
                        raise

                elif action == "shutdown":
                    logger.info("shutdown_requested")
                    await self._cleanup()

                    # Acknowledge - no payload
                    await self._control_socket.send_multipart([b"", b"shutdown_complete"])
                    break
        except (asyncio.CancelledError, KeyboardInterrupt):
            # Handle graceful shutdown on interrupt
            await self._cleanup()
        finally:
            self._control_socket.close()

    async def _cleanup(self):
        """Cleanup all devices."""
        for device_id, server in self._device_servers.items():
            try:
                server.close()
            except Exception as e:
                logger.error("error_closing_device", device_id=device_id, error=str(e))

        self._device_servers.clear()

        # Close log socket
        if self._log_socket:
            self._log_socket.close()


async def run_node_service(
    service_cls: type[NodeService] = NodeService,
    node_id: str | None = None,
    controller_addr: str | None = None,
    ctrl_host: str = "localhost",
    ctrl_port: int = 9000,
    log_port: int = 9001,
    start_port: int = 10000,
):
    """Run a node service with the given class.

    Args:
        service_cls: NodeService class to instantiate (default: NodeService)
        node_id: Node identifier (if None, read from sys.argv[1])
        controller_addr: Controller address (if None, read from sys.argv[2] or use default)
        log_port: Port for log aggregation (default: 9001)
        start_port: Starting port for device allocation
    """
    if node_id is None:
        if len(sys.argv) < 2:
            print("Usage: python -m pyrig.node <node_id> [controller_host] [control_port] [log_port]")
            print("Example: python -m pyrig.node camera_1 localhost 9000 9001")
            sys.exit(1)
        node_id = sys.argv[1]

    if controller_addr is None:
        controller_addr = sys.argv[2] if len(sys.argv) >= 3 else "tcp://localhost:9000"

    logger.info("starting_node_service", service_class=service_cls.__name__, node_id=node_id)
    logger.info("connecting_to_controller", controller_host=ctrl_host, ctrl_port=ctrl_port, log_port=log_port)

    zctx = zmq.asyncio.Context()
    node = service_cls(zctx, node_id, ctrl_host, ctrl_port, log_port, start_port)

    try:
        await node.run()
    except KeyboardInterrupt:
        logger.info("shutting_down")


async def main():
    """Entry point for base NodeService.

    Usage:
        python -m pyrig.node <node_id> [controller_addr]

    Examples:
        python -m pyrig.node camera_1
        python -m pyrig.node camera_1 tcp://192.168.1.100:9000
    """
    await run_node_service()


if __name__ == "__main__":
    asyncio.run(main())
