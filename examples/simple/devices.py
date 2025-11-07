"""Simple device implementations for the basic PyRig example.

These devices demonstrate PyRig's capabilities with domain-relevant examples:
- TemperatureController: Environmental control
- MotorStage: Motion control
- Pump: Fluid handling
"""

import random
from pyrig.device import Device, DeviceType, describe


class TemperatureController(Device):
    """Temperature controller for environmental control."""

    __DEVICE_TYPE__ = DeviceType.OTHER
    __COMMANDS__ = {"start_regulation", "stop_regulation", "reset"}

    def __init__(self, uid: str, target_temp: float = 25.0):
        super().__init__(uid=uid)
        self._target_temp = target_temp
        self._current_temp = 20.0
        self._is_regulating = False
        self._power_percent = 0.0

    @property
    @describe(label="Target Temperature", units="°C")
    def target_temperature(self) -> float:
        """Get target temperature setpoint."""
        return self._target_temp

    @target_temperature.setter
    def target_temperature(self, value: float) -> None:
        """Set target temperature setpoint."""
        if value < -50 or value > 150:
            raise ValueError("Temperature must be between -50°C and 150°C")
        self._target_temp = value

    @property
    @describe(label="Current Temperature", units="°C")
    def current_temperature(self) -> float:
        """Get current measured temperature."""
        # Simulate temperature drift towards target
        if self._is_regulating:
            diff = self._target_temp - self._current_temp
            self._current_temp += diff * 0.1 + random.uniform(-0.2, 0.2)
        return round(self._current_temp, 2)

    @property
    @describe(label="Regulation Active")
    def is_regulating(self) -> bool:
        """Check if temperature regulation is active."""
        return self._is_regulating

    @property
    @describe(label="Heater Power", units="%")
    def heater_power(self) -> float:
        """Get current heater power output."""
        if self._is_regulating:
            diff = self._target_temp - self._current_temp
            self._power_percent = max(0, min(100, diff * 10))
        else:
            self._power_percent = 0.0
        return round(self._power_percent, 1)

    @describe(label="Start Regulation", desc="Start temperature regulation")
    def start_regulation(self) -> str:
        """Start active temperature regulation."""
        self._is_regulating = True
        self.log.info(f"Temperature regulation started towards {self._target_temp}°C")
        return f"Temperature regulation started (target: {self._target_temp}°C)"

    @describe(label="Stop Regulation", desc="Stop temperature regulation")
    def stop_regulation(self) -> str:
        """Stop temperature regulation."""
        self._is_regulating = False
        self._power_percent = 0.0
        return "Temperature regulation stopped"

    @describe(label="Reset", desc="Reset to ambient temperature")
    def reset(self) -> str:
        """Reset controller to ambient temperature."""
        self._is_regulating = False
        self._current_temp = 20.0
        self._target_temp = 25.0
        self._power_percent = 0.0
        return "Controller reset to ambient conditions"


class MotorStage(Device):
    """Single-axis motorized stage for positioning."""

    __DEVICE_TYPE__ = DeviceType.OTHER
    __COMMANDS__ = {"move_absolute", "move_relative", "home", "stop"}

    def __init__(self, uid: str, axis: str = "X", max_position: float = 100.0):
        super().__init__(uid=uid)
        self._axis = axis
        self._max_position = max_position
        self._position = 0.0
        self._is_moving = False
        self._velocity = 10.0  # mm/s
        self._homed = False

    @property
    @describe(label="Axis Name")
    def axis(self) -> str:
        """Get axis identifier."""
        return self._axis

    @property
    @describe(label="Position", units="mm")
    def position(self) -> float:
        """Get current position."""
        return round(self._position, 3)

    @property
    @describe(label="Is Moving")
    def is_moving(self) -> bool:
        """Check if stage is currently moving."""
        return self._is_moving

    @property
    @describe(label="Is Homed")
    def is_homed(self) -> bool:
        """Check if stage has been homed."""
        return self._homed

    @property
    @describe(label="Velocity", units="mm/s")
    def velocity(self) -> float:
        """Get movement velocity."""
        return self._velocity

    @velocity.setter
    def velocity(self, value: float) -> None:
        """Set movement velocity."""
        if value <= 0 or value > 50:
            raise ValueError("Velocity must be between 0 and 50 mm/s")
        self._velocity = value

    @describe(label="Move Absolute", desc="Move to absolute position")
    def move_absolute(self, position: float) -> str:
        """Move to absolute position in mm."""
        if not self._homed:
            raise RuntimeError("Stage must be homed before moving")
        if position < 0 or position > self._max_position:
            raise ValueError(f"Position must be between 0 and {self._max_position} mm")

        self._is_moving = True
        self._position = position  # Simulate instant move
        self._is_moving = False
        return f"Moved to {position} mm"

    @describe(label="Move Relative", desc="Move relative to current position")
    def move_relative(self, distance: float) -> str:
        """Move relative to current position in mm."""
        new_pos = self._position + distance
        return self.move_absolute(new_pos)

    @describe(label="Home", desc="Home the stage")
    def home(self) -> str:
        """Home the stage to reference position."""
        self._is_moving = True
        self._position = 0.0
        self._homed = True
        self._is_moving = False
        return "Homing complete"

    @describe(label="Stop", desc="Emergency stop")
    def stop(self) -> str:
        """Stop all motion immediately."""
        self._is_moving = False
        return f"Stopped at position {self._position} mm"


class Pump(Device):
    """Peristaltic pump for fluid handling."""

    __DEVICE_TYPE__ = DeviceType.OTHER
    __COMMANDS__ = {"start", "stop", "dispense_volume"}

    def __init__(self, uid: str, max_flow_rate: float = 100.0):
        super().__init__(uid=uid)
        self._max_flow_rate = max_flow_rate
        self._flow_rate = 10.0
        self._is_running = False
        self._total_volume = 0.0

    @property
    @describe(label="Flow Rate", units="mL/min")
    def flow_rate(self) -> float:
        """Get current flow rate setpoint."""
        return self._flow_rate

    @flow_rate.setter
    def flow_rate(self, value: float) -> None:
        """Set flow rate setpoint."""
        if value < 0 or value > self._max_flow_rate:
            raise ValueError(f"Flow rate must be between 0 and {self._max_flow_rate} mL/min")
        self._flow_rate = value

    @property
    @describe(label="Is Running")
    def is_running(self) -> bool:
        """Check if pump is currently running."""
        return self._is_running

    @property
    @describe(label="Total Volume", units="mL")
    def total_volume_dispensed(self) -> float:
        """Get total volume dispensed since reset."""
        return round(self._total_volume, 2)

    @describe(label="Start", desc="Start continuous pumping")
    def start(self) -> str:
        """Start continuous pumping at current flow rate."""
        self._is_running = True
        return f"Pump started at {self._flow_rate} mL/min"

    @describe(label="Stop", desc="Stop pumping")
    def stop(self) -> str:
        """Stop pumping."""
        self._is_running = False
        return "Pump stopped"

    @describe(label="Dispense Volume", desc="Dispense specific volume")
    def dispense_volume(self, volume_ml: float) -> str:
        """Dispense a specific volume in mL."""
        if volume_ml <= 0:
            raise ValueError("Volume must be positive")

        self._total_volume += volume_ml
        return f"Dispensed {volume_ml} mL (total: {self._total_volume} mL)"

    @describe(label="Reset Counter", desc="Reset volume counter")
    def reset_counter(self) -> str:
        """Reset total volume counter."""
        self._total_volume = 0.0
        return "Volume counter reset"
