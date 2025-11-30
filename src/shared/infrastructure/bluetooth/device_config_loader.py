import json
import logging
from typing import List, Dict
from pathlib import Path
from .bluetooth_device import BluetoothDevice

logger = logging.getLogger(__name__)


class DeviceConfigLoader:
    """
    Loads and hot-reloads bluetooth device configuration from JSON

    Supports dynamic device addition/removal without restart
    """

    def __init__(self, config_file: str):
        """
        Initialize loader

        Args:
            config_file: Path to bluetooth_devices.json
        """
        self.config_file = Path(config_file)
        self.devices: Dict[str, BluetoothDevice] = {}
        self.last_modified: float = 0

    def load_devices(self, force_reload: bool = False) -> List[BluetoothDevice]:
        """
        Load devices from config file

        Checks file modification time and only reloads if changed.

        Args:
            force_reload: Force reload even if file hasn't changed

        Returns:
            List of BluetoothDevice objects
        """
        try:
            # Check if file exists
            if not self.config_file.exists():
                logger.error(f"Config file not found: {self.config_file}")
                return list(self.devices.values())

            # Check if file was modified
            current_mtime = self.config_file.stat().st_mtime

            if not force_reload and current_mtime == self.last_modified:
                # File hasn't changed, return cached devices
                return list(self.devices.values())

            # File changed or forced reload, read it
            logger.info(f"Loading device config from: {self.config_file}")

            with open(self.config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)

            devices_data = config_data.get('devices', [])

            # Track existing devices to preserve state
            existing_devices = {d.device_identifier: d for d in self.devices.values()}

            # Load new configuration
            new_devices = {}

            for device_data in devices_data:
                device_identifier = device_data.get('deviceIdentifier')
                port = device_data.get('port')

                if not device_identifier or not port:
                    logger.warning(f"Invalid device config: {device_data}")
                    continue

                # Preserve state if device already exists
                if device_identifier in existing_devices:
                    device = existing_devices[device_identifier]
                    # Update port in case it changed
                    device.port = port
                else:
                    # New device
                    device = BluetoothDevice(
                        device_identifier=device_identifier,
                        port=port
                    )

                new_devices[device_identifier] = device

            # Update cached devices
            self.devices = new_devices
            self.last_modified = current_mtime

            logger.info(f"Loaded {len(self.devices)} device(s)")
            for device in self.devices.values():
                logger.info(f"  - {device}")

            return list(self.devices.values())

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {e}")
            return list(self.devices.values())

        except Exception as e:
            logger.error(f"Error loading device config: {e}", exc_info=True)
            return list(self.devices.values())

    def get_device(self, device_identifier: str) -> BluetoothDevice:
        """
        Get device by identifier

        Args:
            device_identifier: Device identifier

        Returns:
            BluetoothDevice or None if not found
        """
        return self.devices.get(device_identifier)

    def reload_if_changed(self) -> bool:
        """
        Reload config if file changed

        Returns:
            True if config was reloaded, False otherwise
        """
        if not self.config_file.exists():
            return False

        current_mtime = self.config_file.stat().st_mtime

        if current_mtime != self.last_modified:
            logger.info("Config file changed, reloading...")
            self.load_devices(force_reload=True)
            return True

        return False