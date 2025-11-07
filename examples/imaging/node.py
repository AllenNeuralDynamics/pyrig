"""Entry point for running an ImagingNodeService on a remote host.

This allows you to run imaging-specific node services (with CameraService support)
on remote machines that connect to a central ImagingRig controller.

Usage:
    python -m imaging.node <node_id> [controller_host] [control_port] [log_port] [start_port]

Examples:
    # Connect to controller on localhost
    python -m imaging.node camera_node_1

    # Connect to remote controller
    python -m imaging.node camera_node_1 192.168.1.100 9000 9001
"""

from imaging.drivers.camera import Camera, CameraService
from pyrig import Device, NodeService
from pyrig.node import main


class ImagingNodeService(NodeService):
    """Custom NodeService that handles Camera devices with streaming support."""

    def _create_service(self, device: Device, conn):
        """Hook for custom service types."""
        if isinstance(device, Camera):
            return CameraService(device, conn, self._zctx)
        return super()._create_service(device, conn)


if __name__ == "__main__":
    main(ImagingNodeService)
