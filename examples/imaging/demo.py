import asyncio
import sys
from pathlib import Path
import structlog

import zmq
import zmq.asyncio
from rich import print

from imaging.rig import ImagingRig
from pyrig import RigConfig, configure_console_logging

logger = structlog.get_logger()
configure_console_logging(level="INFO")


async def main():
    """Entry point for primary controller."""

    if len(sys.argv) < 2:
        default_config_path = Path(__file__).parent / "system.yaml"
        print(f"[yellow]No config file provided. Using default: {default_config_path}[/yellow]")
        sys.argv.append(str(default_config_path))

    config_path = Path(sys.argv[1])

    if not config_path.exists():
        print(f"[red]Config file not found: {config_path}[/red]")
        sys.exit(1)

    # Load configuration
    config = RigConfig.from_yaml(config_path)

    # Create controller
    zctx = zmq.asyncio.Context()
    controller = ImagingRig(zctx, config)

    try:
        # Start rig
        await controller.start()

        # Example: List all devices
        print("\n[cyan]Available devices:[/cyan]")
        for device_id, agent in controller.devices.items():
            print(f"  - {device_id}")
            interface = await agent.get_interface()
            print(interface)

        # Showcase typed LaserClient API
        if controller.lasers:
            print("\n[cyan]=== Demonstrating Typed LaserClient API ===[/cyan]")
            laser_id = next(iter(controller.lasers.keys()))
            laser = controller.lasers[laser_id]

            print(f"\n[yellow]Working with laser: {laser_id}[/yellow]")

            # Type-safe property access
            print(f"Initial power setpoint: {await laser.get_power_setpoint()}")
            print(f"Laser is on: {await laser.get_is_on()}")

            # Type-safe command calls with autocomplete
            print("\nSetting power to 50.0...")
            await laser.set_power_setpoint(50.0)
            print(f"New power setpoint: {await laser.get_power_setpoint()}")

            print("\nTurning laser on...")
            result = await laser.turn_on()
            print(f"Turn on result: {result}")
            print(f"Laser is on: {await laser.get_is_on()}")

            print("\nUsing combined command set_power_and_on(75.0)...")
            msg = await laser.set_power_and_on(75.0)
            print(f"Result: {msg}")
            print(f"New power setpoint: {await laser.get_power_setpoint()}")

            print("\nTurning laser off...")
            await laser.turn_off()
            print(f"Laser is on: {await laser.get_is_on()}")

            print("\n[green]✓ All typed LaserClient methods work with full autocomplete![/green]")

        # Showcase typed CameraClient API with service-level commands
        if controller.cameras:
            print("\n[cyan]=== Demonstrating Typed CameraClient API ===[/cyan]")
            camera_id = next(iter(controller.cameras.keys()))
            camera = controller.cameras[camera_id]

            print(f"\n[yellow]Working with camera: {camera_id}[/yellow]")

            # Type-safe property access
            print(f"Pixel size: {await camera.get_pixel_size()} µm")
            print(f"Initial exposure time: {await camera.get_exposure_time()} ms")
            print(f"Frame time: {await camera.get_frame_time()} ms")

            # Set exposure
            print("\nSetting exposure time to 50.0 ms...")
            await camera.set_exposure_time(50.0)
            print(f"New exposure time: {await camera.get_exposure_time()} ms")
            print(f"New frame time: {await camera.get_frame_time()} ms")

            # Service-level command (streaming)
            print("\nTesting service-level streaming command...")
            result = await camera.start_stream(num_frames=5)
            print(f"Stream result: {result}")

            print("\n[green]✓ CameraClient with service-level commands working![/green]")

        # # Keep running
        # print("\n[cyan]Rig ready! Press Ctrl+C to exit.[/cyan]")
        # try:
        #     await asyncio.Event().wait()
        # except KeyboardInterrupt:
        #     print("\n[yellow]Shutting down...[/yellow]")
    finally:
        await controller.stop()


if __name__ == "__main__":
    asyncio.run(main())
