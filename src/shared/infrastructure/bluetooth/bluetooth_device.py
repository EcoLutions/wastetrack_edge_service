from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class BluetoothDevice:
    """
    Represents a Bluetooth device configuration

    Loaded from bluetooth_devices.json
    """

    device_identifier: str
    port: str
    is_online: bool = False
    last_seen: Optional[datetime] = None
    consecutive_failures: int = 0

    def mark_online(self):
        """Mark device as online"""
        self.is_online = True
        self.last_seen = datetime.now()
        self.consecutive_failures = 0

    def mark_offline(self):
        """Mark device as offline"""
        self.is_online = False
        self.consecutive_failures += 1

    def __repr__(self) -> str:
        status = "ONLINE" if self.is_online else "OFFLINE"
        return f"BluetoothDevice({self.device_identifier}, {self.port}, {status})"