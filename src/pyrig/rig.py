"""Primary controller for rig orchestration."""

import asyncio
import json
from multiprocessing import Process

import structlog
import zmq
import zmq.asyncio

from pyrig.config import RigConfig
from pyrig.device import DeviceClient
from pyrig.node import NodeService, ProvisionComplete, ProvisionedDevice, ProvisionResponse

# Logger will be configured by user via configure_rig_logging()
logger = structlog.get_logger(__name__)


def configure_console_logging(level: str = "INFO"):
    """Configure structlog for the rig and return self.log.

    Args:
        level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger for the rig
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.dev.ConsoleRenderer(),  # Pretty console output
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level.lower()),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def _run_node_service(node_id: str, ctrl_port: int, log_port: int, start_port: int, service_cls: type[NodeService]):
    """Run NodeService in a subprocess.

    This function is the target for subprocess.Process.

    Args:
        node_id: Unique identifier for this node
        controller_addr: Address of controller ROUTER socket
    """
    import asyncio

    import zmq.asyncio

    async def _run():
        zctx = zmq.asyncio.Context()
        node = service_cls(zctx, node_id, "localhost", ctrl_port=ctrl_port, log_port=log_port, start_port=start_port)
        await node.run()

    asyncio.run(_run())


# Architecture:
# - Controller starts ROUTER socket on control_port (single port for all nodes)
# - Controller spawns local node subprocesses
# - All nodes (local and remote) connect to controller:control_port using DEALER sockets
# - Nodes set their ZMQ identity to node_id
# - Nodes request config via "provision" action
# - Controller sends NodeConfig to each node
# - Nodes create devices and allocate ports automatically
# - Nodes respond with device addresses via "provision_complete" action
# - Controller creates DeviceClients and waits for heartbeats


class Rig:
    """Primary controller that orchestrates the entire rig."""

    NODE_SERVICE_CLASS: type[NodeService] = NodeService

    def __init__(self, zctx: zmq.asyncio.Context, config: RigConfig):
        self.log = structlog.get_logger().bind(component="rig")
        self.zctx = zctx
        self.config = config
        self.provisions: dict[str, ProvisionedDevice] = {}

        self.devices: dict[str, DeviceClient] = {}

        self._local_nodes: dict[str, Process] = {}
        self._control_socket = self.zctx.socket(zmq.ROUTER)
        self._log_socket = self.zctx.socket(zmq.SUB)
        self._log_socket.subscribe(b"")  # Subscribe to all log messages
        self._log_watch_task = None

    async def start(self, connection_timeout: float = 30.0, provision_timeout: float = 30.0):
        """Complete startup sequence.

        Args:
            connection_timeout: How long to wait for all devices to connect
            provision_timeout: How long to wait for nodes to provision
        """
        # Configure structlog if not already configured
        if not structlog.is_configured():
            configure_console_logging()

        self.log.info("starting_rig", rig_name=self.config.metadata.name)

        # Step 1: Start control listener and log aggregator
        control_port = self.config.metadata.control_port
        self._control_socket.bind(f"tcp://*:{control_port}")

        log_port = self.config.metadata.log_port
        self._log_socket.bind(f"tcp://*:{log_port}")
        self.log.info("log_aggregator_listening", port=log_port)

        # Start log receiver task
        self._log_watch_task = asyncio.create_task(self._receive_logs())

        # Step 2: Start local node subprocesses
        self._local_nodes = await self._spawn_local_nodes()

        # Step 3: Provision all nodes (local and remote) and get device connections
        self.provisions = await self._provision_nodes(timeout=provision_timeout)

        # Step 4: Create typed DeviceClients based on device type
        self.create_clients()

        # Step 5: Wait for all devices to send heartbeats
        await self._wait_for_connections(timeout=connection_timeout)

        self.log.info("rig_ready", rig_name=self.config.metadata.name, device_count=len(self.devices))

    def create_clients(self) -> None:
        """Create DeviceClients from provisions. Override to customize client types."""
        for device_id, prov in self.provisions.items():
            client = self._create_client(device_id, prov)
            self.devices[device_id] = client

    def _create_client(self, device_id: str, prov: ProvisionedDevice) -> DeviceClient:
        """Create a single client. Override for custom client types."""
        return DeviceClient(uid=device_id, zctx=self.zctx, conn=prov.conn)

    async def _spawn_local_nodes(self) -> dict[str, Process]:
        """Spawn local node subprocesses."""
        local_nodes = self.config.local_nodes

        if not local_nodes:
            return {}

        ctrl_port = self.config.metadata.control_port
        log_port = self.config.metadata.log_port

        processes = {}
        start_port = 10000
        for node_id in local_nodes.keys():
            # Create subprocess running NodeService
            process = Process(
                target=_run_node_service,
                args=(node_id, ctrl_port, log_port, start_port, self.NODE_SERVICE_CLASS),
                name=f"node-{node_id}",
                daemon=True,
            )
            process.start()
            processes[node_id] = process
            start_port += 1000

        # Give processes time to start up and connect
        await asyncio.sleep(0.5)

        return processes

    async def _provision_nodes(self, timeout: float = 30.0) -> dict[str, ProvisionedDevice]:
        """Provision all nodes (local and remote).

        Arguments:
            timeout: How long to wait for all nodes to provision

        Returns:
            Dictionary mapping device_id to DeviceAddressTCP

        Raises:
            RuntimeError: If provisioning fails or times out
        """
        all_devices: dict[str, ProvisionedDevice] = {}
        expected_nodes = set(self.config.nodes.keys())
        provisioned_nodes: set[str] = set()

        try:
            async with asyncio.timeout(timeout):
                while provisioned_nodes != expected_nodes:
                    parts = await self._control_socket.recv_multipart()
                    identity = parts[0]
                    node_id = identity.decode()
                    action = parts[2].decode()

                    if action == "provision":
                        # Node is requesting config
                        if node_id not in self.config.nodes:
                            self.log.error("unknown_node", node_id=node_id)
                            continue

                        node_config = self.config.nodes[node_id]
                        response = ProvisionResponse(config=node_config)

                        await self._control_socket.send_multipart(
                            [
                                identity,
                                b"",
                                b"provision",
                                response.model_dump_json().encode(),
                            ]
                        )

                    elif action == "provision_complete":
                        # Node reporting successful provisioning
                        payload = parts[3]
                        complete = ProvisionComplete.model_validate_json(payload)
                        all_devices.update(complete.devices)
                        provisioned_nodes.add(node_id)

        except asyncio.TimeoutError:
            missing = expected_nodes - provisioned_nodes
            self.log.error("provisioning_timeout", missing_nodes=list(missing))

            await self._shutdown_nodes(provisioned_nodes)

            raise RuntimeError(
                f"Rig startup failed. Missing nodes: {missing}. All provisioned nodes have been shut down."
            )

        return all_devices

    async def _shutdown_nodes(self, node_ids: set[str], timeout: float = 5.0):
        """Send shutdown command to specific nodes and wait for acknowledgment.

        Args:
            node_ids: Set of node IDs to shutdown
            timeout: How long to wait for shutdown acknowledgments
        """
        if not node_ids:
            return

        # Send shutdown to all nodes
        for node_id in node_ids:
            identity = node_id.encode()
            try:
                await self._control_socket.send_multipart([identity, b"", b"shutdown"])
            except Exception as e:
                self.log.error("shutdown_send_failed", node_id=node_id, error=str(e))

        # Wait for shutdown acknowledgments
        acknowledged = set()
        try:
            async with asyncio.timeout(timeout):
                while acknowledged != node_ids:
                    parts = await self._control_socket.recv_multipart()
                    identity = parts[0]
                    node_id = identity.decode()
                    action = parts[1].decode()

                    if action == "shutdown_complete":
                        acknowledged.add(node_id)
        except asyncio.TimeoutError:
            pass  # Timeout is expected if nodes were interrupted

        # Force-terminate local subprocesses
        for node_id in node_ids:
            if node_id in self._local_nodes:
                process = self._local_nodes[node_id]
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2.0)
                    if process.is_alive():
                        process.kill()
                        process.join()

    async def _wait_for_connections(self, timeout: float = 30.0):
        """Wait for all agents to receive heartbeats from their devices."""
        # Use wait_for_connection() method from DeviceClient
        tasks = {device_id: agent.wait_for_connection(timeout=timeout) for device_id, agent in self.devices.items()}

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        # Check results
        failed = []
        for device_id, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                self.log.error("device_connection_error", device_id=device_id, error=str(result))
                failed.append(device_id)
            elif not result:  # wait_for_connection returned False
                self.log.warning("device_connection_timeout", device_id=device_id)
                failed.append(device_id)

        if failed:
            self.log.warning("devices_not_connected", failed_count=len(failed), total_count=len(self.devices))

    async def _receive_logs(self):
        """Background task to receive JSON logs from nodes and forward to structlog."""
        try:
            while True:
                # Receive JSON log message
                log_data = await self._log_socket.recv_json()

                # Extract level and event from the log data
                level = log_data.get("level", "info").lower()
                event = log_data.get("event", "")

                # Remove level and event from data to use as context
                context = {k: v for k, v in log_data.items() if k not in ("level", "event")}

                # Log with appropriate level using the main logger
                log_method = getattr(logger, level, self.log.info)
                log_method(event, **context)

        except asyncio.CancelledError:
            pass  # Task cancelled during shutdown
        except json.JSONDecodeError as e:
            self.log.error("json_decode_error", error=str(e))
        except Exception as e:
            self.log.error("log_receiver_error", error=str(e))

    def get_agent(self, device_id: str) -> DeviceClient:
        """Get agent for a specific device.

        Args:
            device_id: Device identifier

        Returns:
            DeviceClient instance

        Raises:
            KeyError: If device not found
        """
        return self.devices[device_id]

    async def stop(self):
        """Stop all devices and cleanup."""
        # Step 1: Stop log receiver
        if self._log_watch_task:
            self._log_watch_task.cancel()
            try:
                await self._log_watch_task
            except asyncio.CancelledError:
                pass

        # Step 2: Close all DeviceClients first (disconnect from devices)
        for device_id, agent in self.devices.items():
            agent.close()

        # Step 3: Shutdown all nodes (send shutdown, wait for ack, terminate processes)
        all_nodes = set(self.config.nodes.keys())
        await self._shutdown_nodes(all_nodes, timeout=2.0)

        # Step 4: Close sockets
        self._control_socket.close()
        self._log_socket.close()
