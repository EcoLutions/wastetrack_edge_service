import logging
from typing import Optional, List
from src.containermonitoring.domain.model.aggregates import ContainerConfig
from src.containermonitoring.infrastructure.persistence import ContainerConfigRepository

logger = logging.getLogger(__name__)


class ContainerConfigService:
    """
    Application Service for ContainerConfig operations

    Responsibilities:
    - Retrieve container configurations
    - Determine alert thresholds
    - Validate container configurations
    """

    def __init__(self, container_config_repository: ContainerConfigRepository):
        """
        Initialize service with dependencies

        Args:
            container_config_repository: Repository for config persistence
        """
        self.container_config_repository = container_config_repository

    def get_config_by_container_id(self, container_id: str) -> Optional[ContainerConfig]:
        """
        Get configuration for a specific container

        Args:
            container_id: UUID of the container

        Returns:
            ContainerConfig aggregate or None if not found
        """
        return self.container_config_repository.find_by_id(container_id)

    def get_config_by_sensor_id(self, sensor_id: str) -> Optional[ContainerConfig]:
        """
        Get configuration for a specific sensor

        Useful when processing sensor readings to determine the threshold.

        Args:
            sensor_id: UUID of the sensor

        Returns:
            ContainerConfig aggregate or None if not found
        """
        return self.container_config_repository.find_by_sensor_id(sensor_id)

    def get_threshold_for_container(self, container_id: str) -> Optional[float]:
        """
        Get the fill level threshold for a container

        Args:
            container_id: UUID of the container

        Returns:
            Threshold percentage (0-100) or None if config is not found
        """
        config = self.container_config_repository.find_by_id(container_id)

        if config:
            return config.max_fill_level_threshold
        else:
            logger.warning(f"No config found for container: {container_id}")
            return None

    def is_fill_level_alert(self, container_id: str,
                            fill_level_percentage: float) -> bool:
        """
        Determine if a fill level should trigger an alert

        Business rule:
        - fillLevel >= threshold → ALERT
        - fillLevel < threshold → Normal

        Args:
            container_id: UUID of the container
            fill_level_percentage: Current fill level (0-100)

        Returns:
            True if an alert should be triggered, False otherwise
        """
        config = self.container_config_repository.find_by_id(container_id)

        if not config:
            logger.warning(
                f"No config found for container {container_id}, "
                f"cannot determine alert status"
            )
            return False

        is_alert = config.is_full(fill_level_percentage)

        if is_alert:
            logger.info(
                f"🚨 ALERT: Container {container_id} at {fill_level_percentage}% "
                f"(threshold: {config.max_fill_level_threshold}%)"
            )
        else:
            logger.debug(
                f"Normal: Container {container_id} at {fill_level_percentage}% "
                f"(threshold: {config.max_fill_level_threshold}%)"
            )

        return is_alert

    def get_all_configs(self) -> List[ContainerConfig]:
        """
        Get all container configurations

        Returns:
            List of all ContainerConfig aggregates
        """
        return self.container_config_repository.find_all()

    def get_config_count(self) -> int:
        """
        Get the total number of container configurations

        Returns:
            Count of configs
        """
        return self.container_config_repository.count()

    def has_config(self, container_id: str) -> bool:
        """
        Check if a container has a configuration

        Args:
            container_id: UUID of the container

        Returns:
            True if config exists, False otherwise
        """
        config = self.container_config_repository.find_by_id(container_id)
        return config is not None