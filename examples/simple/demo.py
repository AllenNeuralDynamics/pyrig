"""Simple demo using base Rig class without any customization.

This example shows how to use PyRig with the base classes directly,
without creating custom Rig or NodeService subclasses. All devices
are accessed through the generic `agents` dictionary.

Usage:
    cd examples
    python -m simple.demo [config_file]
"""

import asyncio
import sys
from pathlib import Path

import structlog
import zmq.asyncio
from rich import print

from pyrig import Rig, RigConfig, configure_console_logging

logger = structlog.get_logger()
configure_console_logging(level="INFO")


async def main():
    """Simple demo using base Rig class."""

    # Get config file path
    if len(sys.argv) < 2:
        config_path = Path(__file__).parent / "system.yaml"
        print(f"[yellow]No config file provided. Using: {config_path}[/yellow]")
    else:
        config_path = Path(sys.argv[1])

    if not config_path.exists():
        print(f"[red]Config file not found: {config_path}[/red]")
        sys.exit(1)

    # Load configuration
    config = RigConfig.from_yaml(config_path)

    # Create base Rig (no customization)
    zctx = zmq.asyncio.Context()
    rig = Rig(zctx, config)

    print(f"\n[bold cyan]=== Starting {config.metadata.name} ===[/bold cyan]")
    print("[cyan]Using base Rig class (no custom clients)[/cyan]\n")

    try:
        # Start the rig
        await rig.start()

        # List all connected devices
        print("\n[bold green]✓ Rig started successfully![/bold green]")
        print(f"[cyan]Connected devices: {len(rig.devices)}[/cyan]\n")

        # Show device interfaces
        print("[bold]Device Interfaces:[/bold]")
        for device_id, agent in rig.devices.items():
            print(f"\n[yellow]Device: {device_id}[/yellow]")
            interface = await agent.get_interface()
            print(f"  Type: {interface.type}")
            print(f"  Commands: {len(interface.commands)}")
            print(f"  Properties: {len(interface.properties)}")

            # Show some commands
            if interface.commands:
                print("  Available commands:")
                for cmd_name in list(interface.commands.keys())[:3]:
                    print(f"    - {cmd_name}")

        # Demonstrate generic device access
        print("\n[bold cyan]=== Demonstrating Generic Device Access ===[/bold cyan]\n")

        # Work with temperature controller
        if "temp_controller" in rig.devices:
            temp = rig.devices["temp_controller"]

            print("[yellow]Working with Temperature Controller[/yellow]\n")

            # Get initial state
            props = await temp.get_props()
            print(f"Target: {props.res['target_temperature'].value}°C")
            print(f"Current: {props.res['current_temperature'].value}°C")
            print(f"Regulating: {props.res['is_regulating'].value}")

            # Start regulation
            print("\n[cyan]Starting temperature regulation...[/cyan]")
            result = await temp.call("start_regulation")
            print(f"  {result}")

            # Check updated state
            props = await temp.get_props()
            print(f"Heater power: {props.res['heater_power'].value}%")

        # Work with motor stage
        if "x_stage" in rig.devices:
            stage = rig.devices["x_stage"]

            print("\n[yellow]Working with Motor Stage (X-axis)[/yellow]\n")

            # Home the stage
            print("[cyan]Homing stage...[/cyan]")
            result = await stage.call("home")
            print(f"  {result}")

            # Get properties
            props = await stage.get_props()
            print(f"Position: {props.res['position'].value} mm")
            print(f"Homed: {props.res['is_homed'].value}")

            # Move to position
            print("\n[cyan]Moving to 25.0 mm...[/cyan]")
            result = await stage.call("move_absolute", 25.0)
            print(f"  {result}")

            props = await stage.get_props()
            print(f"New position: {props.res['position'].value} mm")

        # Work with pump
        if "pump_1" in rig.devices:
            pump = rig.devices["pump_1"]

            print("\n[yellow]Working with Pump[/yellow]\n")

            # Check initial state
            props = await pump.get_props()
            print(f"Flow rate: {props.res['flow_rate'].value} mL/min")
            print(f"Running: {props.res['is_running'].value}")

            # Dispense volume
            print("\n[cyan]Dispensing 5.0 mL...[/cyan]")
            result = await pump.call("dispense_volume", 5.0)
            print(f"  {result}")

            props = await pump.get_props()
            print(f"Total dispensed: {props.res['total_volume_dispensed'].value} mL")

        # # Keep running
        # print("\n[cyan]Press Ctrl+C to exit[/cyan]")
        # try:
        #     await asyncio.Event().wait()
        # except KeyboardInterrupt:
        #     print("\n[yellow]Shutting down...[/yellow]")
    finally:
        await rig.stop()


if __name__ == "__main__":
    asyncio.run(main())
