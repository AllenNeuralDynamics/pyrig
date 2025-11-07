import asyncio
import sys
from pathlib import Path

import structlog
import zmq
import zmq.asyncio

from imaging.rig import ImagingRig
from pyrig import RigConfig, configure_console_logging

configure_console_logging(level="INFO")
logger = structlog.get_logger("demo")


async def main():
    """Demonstration of ImagingRig with typed clients and distributed logging."""

    if len(sys.argv) < 2:
        default_config_path = Path(__file__).parent / "system.yaml"
        logger.info("using_default_config", path=str(default_config_path))
        sys.argv.append(str(default_config_path))

    config_path = Path(sys.argv[1])

    if not config_path.exists():
        logger.error("config_not_found", path=str(config_path))
        sys.exit(1)

    # Load configuration
    config = RigConfig.from_yaml(config_path)

    # Create controller
    zctx = zmq.asyncio.Context()
    controller = ImagingRig(zctx, config)

    try:
        # Start rig
        await controller.start()
        logger.info(
            "demo_ready",
            device_count=len(controller.devices),
            laser_count=len(controller.lasers),
            camera_count=len(controller.cameras),
        )

        # Demonstrate typed LaserClient API
        if controller.lasers:
            logger.info("demonstrating_laser_control")
            laser_id = next(iter(controller.lasers.keys()))
            laser = controller.lasers[laser_id]

            # Type-safe property access
            power_setpoint = await laser.get_power_setpoint()
            is_on = await laser.get_is_on()
            logger.info("laser_status", device=laser_id, power_setpoint=power_setpoint, is_on=is_on)

            # Set power and turn on
            logger.info("setting_laser_power", device=laser_id, power=50.0)
            await laser.set_power_setpoint(50.0)

            logger.info("turning_laser_on", device=laser_id)
            await laser.turn_on()

            is_on = await laser.get_is_on()
            power = await laser.get_power_setpoint()
            logger.info("laser_activated", device=laser_id, power=power, is_on=is_on)

            # Use combined command
            logger.info("using_combined_command", device=laser_id, power=75.0)
            result = await laser.set_power_and_on(75.0)
            logger.info("command_result", device=laser_id, result=result)

            # Turn off
            logger.info("turning_laser_off", device=laser_id)
            await laser.turn_off()
            logger.info("laser_deactivated", device=laser_id)

        # Demonstrate typed CameraClient API with service-level commands
        if controller.cameras:
            logger.info("demonstrating_camera_control")
            camera_id = next(iter(controller.cameras.keys()))
            camera = controller.cameras[camera_id]

            # Type-safe property access
            pixel_size = await camera.get_pixel_size()
            exposure = await camera.get_exposure_time()
            frame_time = await camera.get_frame_time()
            logger.info(
                "camera_properties",
                device=camera_id,
                pixel_size=pixel_size,
                exposure_ms=exposure,
                frame_time_ms=frame_time,
            )

            # Set exposure
            new_exposure = 50.0
            logger.info("setting_exposure", device=camera_id, exposure_ms=new_exposure)
            await camera.set_exposure_time(new_exposure)

            exposure = await camera.get_exposure_time()
            frame_time = await camera.get_frame_time()
            logger.info("exposure_updated", device=camera_id, exposure_ms=exposure, frame_time_ms=frame_time)

            # Service-level command (streaming)
            num_frames = 5
            logger.info("starting_acquisition", device=camera_id, num_frames=num_frames)
            result = await camera.start_stream(num_frames=num_frames)
            logger.info("acquisition_complete", device=camera_id, result=result)

        logger.info("demo_complete")

    finally:
        logger.info("shutting_down_rig")
        await controller.stop()
        logger.info("rig_stopped")


if __name__ == "__main__":
    asyncio.run(main())
