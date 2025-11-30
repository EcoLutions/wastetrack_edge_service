from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ContainerConfig:
    """
    Container Config Aggregate - Container configuration
    """

    container_id: str
    max_fill_level_threshold: float
    sensor_id: str
    last_sync_at: datetime

    def __post_init__(self):
        """Validations after initialization"""
        if not self.container_id:
            raise ValueError("container_id cannot be empty")

        if not self.sensor_id:
            raise ValueError("sensor_id cannot be empty")

        # Validate ranges
        if not (0 <= self.max_fill_level_threshold <= 100):
            raise ValueError(
                f"max_fill_level_threshold must be between 0 and 100, "
                f"got {self.max_fill_level_threshold}"
            )

    @staticmethod
    def from_backend_event(container_id: str, max_fill_level_threshold: float, sensor_id: str, occurred_at: datetime) -> 'ContainerConfig':
        """
        Factory method: Creates ContainerConfig from Backend event

        Args:
            container_id: UUID del container
            max_fill_level_threshold: Fill threshold (0-100)
            sensor_id:  UUID of the associated sensor
            occurred_at: Event timestamp

        Returns:
            ContainerConfig instance
        """
        return ContainerConfig(
            container_id=container_id,
            max_fill_level_threshold=max_fill_level_threshold,
            sensor_id=sensor_id,
            last_sync_at=occurred_at
        )

    def is_full(self, fill_level_percentage: float) -> bool:
        """
        Determines whether a fill level is considered "full."

        Args:
            fill_level_percentage: Current fill level (0-100)

        Returns:
            True if fillLevel >= threshold (it is an alert)
        """
        return fill_level_percentage >= self.max_fill_level_threshold

    def update_threshold(self, new_threshold: float, occurred_at: datetime):
        """
        Update the threshold from a backend event

        Args:
            new_threshold: New threshold (0-100)
            occurred_at: Timestamp of the event
        """
        if not (0 <= new_threshold <= 100):
            raise ValueError(
                f"new_threshold must be between 0 and 100, got {new_threshold}"
            )

        self.max_fill_level_threshold = new_threshold
        self.last_sync_at = occurred_at

    def to_dict(self) -> dict:
        """Serialize to dictionary for persistence"""
        return {
            'container_id': self.container_id,
            'max_fill_level_threshold': self.max_fill_level_threshold,
            'sensor_id': self.sensor_id,
            'last_sync_at': self.last_sync_at.isoformat()
        }

    def __repr__(self) -> str:
        return (
            f"ContainerConfig(container_id='{self.container_id}', "
            f"threshold={self.max_fill_level_threshold}%, "
            f"sensor_id='{self.sensor_id}')"
        )