from pathlib import Path
from pyrig.device import Device, DeviceClient, DeviceService, DeviceType, describe, DeviceAddress
import zmq.asyncio


def parse_tuple_str(str: str) -> tuple[float, float]:
    try:
        x, y = map(float, str.split(","))
        return x, y
    except ValueError:
        raise ValueError("Invalid tuple string format")


class Camera(Device):
    """A mock camera device for testing property and command interfaces."""

    __DEVICE_TYPE__ = DeviceType.CAMERA

    def __init__(self, uid: str, pixel_size_um: str):
        super().__init__(uid=uid)
        self._pixel_size = parse_tuple_str(pixel_size_um)
        self._exposure_time: float = 0.0
        self.log.info("camera_initialized", pixel_size=self._pixel_size)

    @property
    @describe(label="Pixel Size", units="um")
    def pixel_size(self) -> tuple[float, float]:
        """Get the pixel size."""
        return self._pixel_size

    @property
    @describe(label="Frame Time", units="ms")
    def frame_time(self) -> float:
        """Get the frame time in milliseconds."""
        return self._exposure_time * 1.1

    @property
    @describe(label="Exposure Time", units="ms")
    def exposure_time(self) -> float:
        """Get the exposure time in milliseconds."""
        return self._exposure_time

    @exposure_time.setter
    def exposure_time(self, value: float) -> None:
        """Set the exposure time in milliseconds."""
        if value < 0:
            raise ValueError("Exposure time must be non-negative")
        if value > 100:
            raise ValueError("Exposure time cannot exceed 100")
        self.log.debug("exposure_time_changed", old_value=self._exposure_time, new_value=value)
        self._exposure_time = value


class Writer:
    """Simple file writer for camera data."""

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)

    def write(self, text: str) -> None:
        """Append data to the file."""
        with self.file_path.open("a") as f:
            f.write(text)


class CameraService(DeviceService[Camera]):
    """Camera service with extended streaming commands."""

    def __init__(self, device: Camera, conn: DeviceAddress, zctx: zmq.asyncio.Context):
        super().__init__(device, conn, zctx)
        tmp_folder = Path(__file__).parent.parent / "tmp"
        tmp_folder.mkdir(exist_ok=True)
        self._writer = Writer(str(tmp_folder / f"{device.uid}.txt"))
        self.log.info("camera_service_initialized", output_path=str(self._writer.file_path))

    @describe(label="Start Stream", desc="Start streaming frames to file")
    def start_stream(self, num_frames: int = 10) -> str:
        """Start streaming frames."""
        self.log.info("stream_started", num_frames=num_frames)
        self._writer.write(f"Starting stream with {num_frames} frames\n")
        for i in range(num_frames):
            self._writer.write(f"Frame {i + 1}\n")
        self._writer.write("Stream complete\n")
        self.log.info("stream_completed", num_frames=num_frames, output_file=self._writer.file_path.name)
        return f"Streamed {num_frames} frames to {self._writer.file_path.name}"


class CameraClient(DeviceClient):
    """Typed client for Camera devices with streaming support."""

    async def get_exposure_time(self) -> float:
        """Get the camera exposure time in milliseconds."""
        return await self.get_prop("exposure_time")

    async def set_exposure_time(self, value: float) -> None:
        """Set the camera exposure time in milliseconds."""
        await self.set_prop("exposure_time", value)

    async def get_pixel_size(self) -> tuple[float, float]:
        """Get the pixel size in micrometers."""
        return await self.get_prop("pixel_size")

    async def get_frame_time(self) -> float:
        """Get the frame time in milliseconds."""
        return await self.get_prop("frame_time")

    async def start_stream(self, num_frames: int = 10) -> str:
        """Start streaming frames (service-level command)."""
        return await self.call("start_stream", num_frames)
