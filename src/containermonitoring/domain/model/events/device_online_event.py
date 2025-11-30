from dataclasses import dataclass
from datetime import datetime


@dataclass
class DeviceOnlineEvent:
    """
    Domain Event: Device came online

    Published when:
    - an ESP32 device responds to ping after being offline
    - an ESP32 device successfully connects via Bluetooth

    MQTT Topic: cm/devices/events/online

    Payload structure to Backend:
    {
        "eventType": "DEVICE_ONLINE",
        "deviceId": "550e8400-e29b-41d4-a716-446655440000",
        "deviceIdentifier": "SENSOR-001",
        "occurredAt": "2025-11-29T23:45:00"
    }
    """

    device_id: str
    device_identifier: str
    occurred_at: datetime

    def __post_init__(self):
        """Validate event data"""
        if not self.device_id:
            raise ValueError("device_id cannot be empty")

        if not self.device_identifier:
            raise ValueError("device_identifier cannot be empty")

    def to_mqtt_payload(self) -> dict:
        """
        Serialize to MQTT payload for Backend

        Returns:
            Dictionary ready for MQTT publishing
        """
        return {
            "eventType": "DEVICE_ONLINE",
            "deviceId": self.device_id,
            "deviceIdentifier": self.device_identifier,
            "occurredAt": self.occurred_at.isoformat()
        }

    def __repr__(self) -> str:
        return (
            f"DeviceOnlineEvent(device_id='{self.device_id}', "
            f"device_identifier='{self.device_identifier}')"
        )