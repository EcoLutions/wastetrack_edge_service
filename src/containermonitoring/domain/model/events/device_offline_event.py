from dataclasses import dataclass
from datetime import datetime

@dataclass
class DeviceOfflineEvent:
    """
    Domain Event: Device went offline

    Published when:
    - an ESP32 device fails to respond to ping
    - Bluetooth connection timeout
    - Serial communication error

    MQTT Topic: cm/devices/events/offline

    Payload structure to Backend:
    {
        "eventType": "DEVICE_OFFLINE",
        "deviceId": "550e8400-e29b-41d4-a716-446655440000",
        "deviceIdentifier": "SENSOR-001",
        "occurredAt": "2025-11-29T23:45:00",
        "reason": "CONNECTION_TIMEOUT",
        "consecutiveFailures": 3
    }
    """

    device_id: str
    device_identifier: str
    occurred_at: datetime
    reason: str = "CONNECTION_TIMEOUT"
    consecutive_failures: int = 1

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
            "eventType": "DEVICE_OFFLINE",
            "deviceId": self.device_id,
            "deviceIdentifier": self.device_identifier,
            "occurredAt": self.occurred_at.isoformat(),
            "reason": self.reason,
            "consecutiveFailures": self.consecutive_failures
        }

    def __repr__(self) -> str:
        return (
            f"DeviceOfflineEvent(device_id='{self.device_id}', "
            f"device_identifier='{self.device_identifier}', "
            f"reason='{self.reason}')"
        )