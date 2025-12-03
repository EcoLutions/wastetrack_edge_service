from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from enum import Enum


class AlertType(Enum):
    FULL_CONTAINER = "FULL_CONTAINER"
    NONE = "NONE"

@dataclass
class SensorReading:
    """
    Sensor Reading Aggregate - IoT sensor reading
    """
    device_id: str
    container_id: str
    fill_level_percentage: float
    recorded_at: datetime
    received_at: datetime
    synced_to_backend: bool
    synced_at: Optional[datetime] = None
    is_alert: bool = False
    alert_type: AlertType = AlertType.NONE
    id: Optional[int] = None

    def __post_init__(self):
        """Validations after initialization"""
        if not self.device_id:
            raise ValueError("device_id cannot be empty")

        if not self.container_id:
            raise ValueError("container_id cannot be empty")

        # Validate ranges
        if not (0 <= self.fill_level_percentage <= 100):
            raise ValueError(
                f"fill_level_percentage must be between 0 and 100, "
                f"got {self.fill_level_percentage}"
            )

        # If it does not have synced_at and is marked as synchronized, use now
        if self.synced_to_backend and self.synced_at is None:
            self.synced_at = datetime.now()

    @staticmethod
    def from_iot_request(device_id: str, container_id: str, fill_level_percentage: float, recorded_at: datetime, is_alert: bool, alert_type: AlertType) -> 'SensorReading':
        """
        Factory method: Creates SensorReading from IoT request
        """
        return SensorReading(
            device_id=device_id,
            container_id=container_id,
            fill_level_percentage=fill_level_percentage,
            recorded_at=recorded_at,
            received_at=datetime.now(),
            synced_to_backend=False,
            is_alert=is_alert,
            alert_type=alert_type
        )

    def mark_as_synced(self):
        """Mark this reading as synchronized with the backend"""
        self.synced_to_backend = True
        self.synced_at = datetime.now()

    def requires_immediate_sync(self) -> bool:
        """
        Determine whether this reading requires immediate synchronization

        Returns:
            True if it is an alert (must be published to MQTT immediately)
        """
        return self.is_alert

    def to_mqtt_payload(self) -> dict:
        """
        Serialize to send to the backend via MQTT

        Returns:
            Dictionary with structure expected by Backend
        """
        return {
            'deviceId': self.device_id,
            'containerId': self.container_id,
            'fillLevelPercentage': self.fill_level_percentage,
            'recordedAt': self.recorded_at.isoformat(),
            'receivedAt': self.received_at.isoformat(),
            'isAlert': self.is_alert,
            'alertType': self.alert_type.value
        }

    def to_dict(self) -> dict:
        """Serialize to dictionary for persistence in SQLite"""
        return {
            'id': self.id,
            'device_id': self.device_id,
            'container_id': self.container_id,
            'fill_level_percentage': self.fill_level_percentage,
            'recorded_at': self.recorded_at.isoformat(),
            'received_at': self.received_at.isoformat(),
            'synced_to_backend': self.synced_to_backend,
            'synced_at': self.synced_at.isoformat() if self.synced_at else None,
            'is_alert': self.is_alert,
            'alert_type': self.alert_type.value
        }

    def __repr__(self) -> str:
        alert_indicator = " [ALERT]" if self.is_alert else ""
        fill = f"{self.fill_level_percentage:.1f}"
        return (
            f"SensorReading(container={self.container_id!r}, "
            f"fill={fill}%, device={self.device_id!r}{alert_indicator})"
        )