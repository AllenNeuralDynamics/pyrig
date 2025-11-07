import random


from pyrig.device import Device, DeviceClient, DeviceType, describe


class Laser(Device):
    """A mock laser device for testing property and command interfaces."""

    __DEVICE_TYPE__ = DeviceType.LASER
    __COMMANDS__ = {"turn_on", "turn_off"}

    def __init__(self, uid: str, wavelength: float):
        super().__init__(uid=uid)
        self._wavelength = wavelength
        self._power_setpoint: float = 0.0
        self._is_on: bool = False
        self.log.info("laser_initialized", wavelength=wavelength)

    @property
    @describe(label="Wavelength", units="nm")
    def wavelength(self) -> float:
        """Get the laser wavelength."""
        return self._wavelength

    @property
    def is_on(self) -> bool:
        """Get whether the laser is currently on."""
        return self._is_on

    @property
    def power(self) -> float:
        """Get the current laser power output."""
        if not self._is_on:
            return 0.0
        # Mock some power variation when on
        return random.uniform(self._power_setpoint - 0.1, self._power_setpoint + 0.1)

    @property
    def power_setpoint(self) -> float:
        """Get the laser power setpoint."""
        return self._power_setpoint

    @power_setpoint.setter
    def power_setpoint(self, value: float) -> None:
        """Set the laser power setpoint."""
        if value < 0:
            raise ValueError("Power setpoint must be non-negative")
        if value > 100:
            raise ValueError("Power setpoint cannot exceed 100")
        self.log.debug("power_setpoint_changed", old_value=self._power_setpoint, new_value=value)
        self._power_setpoint = value

    def turn_on(self) -> bool:
        """Turn on the laser."""
        if self._power_setpoint <= 0:
            self.log.error("turn_on_failed", reason="zero_power_setpoint")
            raise ValueError("Cannot turn on laser with zero power setpoint")
        self.log.info("laser_turned_on", power_setpoint=self._power_setpoint)
        self._is_on = True
        return True

    def turn_off(self) -> bool:
        """Turn off the laser."""
        self.log.info("laser_turned_off")
        self._is_on = False
        return True

    @describe(label="Set Power", desc="Set laser power and turn on")
    def set_power_and_on(self, power: float) -> str:
        """Set power setpoint and turn on the laser in one command."""
        self.log.info("set_power_and_on", power=power)
        self.power_setpoint = power
        self.turn_on()
        return f"Laser on at {power} power"

    @describe(label="Emergency Stop", desc="Emergency stop - turn off immediately")
    def emergency_stop(self) -> str:
        """Emergency stop the laser."""
        self.log.warning("emergency_stop_triggered")
        self._is_on = False
        self._power_setpoint = 0.0
        return "Emergency stop executed"


class LaserClient(DeviceClient):
    async def turn_on(self) -> bool:
        """Turn on the laser."""
        return await self.call("turn_on")

    async def turn_off(self) -> bool:
        """Turn off the laser."""
        return await self.call("turn_off")

    async def set_power_and_on(self, power: float) -> str:
        """Set laser power and turn on in one command."""
        return await self.call("set_power_and_on", power)

    async def emergency_stop(self) -> str:
        """Emergency stop the laser."""
        return await self.call("emergency_stop")

    async def get_power_setpoint(self) -> float:
        """Get the laser power setpoint."""
        return await self.get_prop("power_setpoint")

    async def set_power_setpoint(self, value: float) -> None:
        """Set the laser power setpoint."""
        await self.set_prop("power_setpoint", value)

    async def get_is_on(self) -> bool:
        """Check if laser is on."""
        return await self.get_prop("is_on")
