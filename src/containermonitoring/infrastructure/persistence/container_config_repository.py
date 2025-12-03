import logging
from typing import Optional, List, Tuple
from datetime import datetime
from peewee import CharField, FloatField, DateTimeField
from src.shared.infrastructure.database import BaseModel, database
from src.containermonitoring.domain.model.aggregates import ContainerConfig

logger = logging.getLogger(__name__)


class ContainerConfigModel(BaseModel):
    """
    Peewee ORM model for container_configs table

    Stores container configurations synced from Backend
    """

    container_id = CharField(primary_key=True, max_length=36)  # UUID
    max_fill_level_threshold = FloatField()
    sensor_id = CharField(max_length=36, index=True)  # UUID
    last_sync_at = DateTimeField()

    class Meta:
        table_name = 'container_configs'


class ContainerConfigRepository:
    """
    Repository for ContainerConfig aggregate

    Handles persistence operations for container configs in SQLite
    """

    def __init__(self):
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create table if it doesn't exist"""
        with database:
            database.create_tables([ContainerConfigModel], safe=True)
        logger.info("ContainerConfigModel table verified/created")

    def save(self, config: ContainerConfig) -> None:
        """
        Save or update a container config

        Args:
            config: ContainerConfig aggregate to persist
        """
        try:
            ContainerConfigModel.replace(
                container_id=config.container_id,
                max_fill_level_threshold=config.max_fill_level_threshold,
                sensor_id=config.sensor_id,
                last_sync_at=config.last_sync_at
            ).execute()

            logger.info(
                f"ContainerConfig saved: {config.container_id} "
                f"(threshold={config.max_fill_level_threshold}%)"
            )

        except Exception as e:
            logger.error(
                f"Error saving container config {config.container_id}: {e}",
                exc_info=True
            )
            raise

    def find_by_id(self, container_id: str) -> Optional[ContainerConfig]:
        """
        Find container config by ID

        Args:
            container_id: UUID of the container

        Returns:
            ContainerConfig aggregate or None if not found
        """
        try:
            model = ContainerConfigModel.get_or_none(
                ContainerConfigModel.container_id == container_id
            )

            if model is None:
                logger.debug(f"ContainerConfig not found: {container_id}")
                return None

            return self._to_aggregate(model)

        except Exception as e:
            logger.error(
                f"Error finding container config by ID {container_id}: {e}",
                exc_info=True
            )
            raise

    def find_by_sensor_id(self, sensor_id: str) -> Optional[ContainerConfig]:
        """
        Find container config by sensor ID

        Useful when processing sensor readings to get the threshold

        Args:
            sensor_id: UUID of the sensor

        Returns:
            ContainerConfig aggregate or None if not found
        """
        try:
            model = ContainerConfigModel.get_or_none(
                ContainerConfigModel.sensor_id == sensor_id
            )

            if model is None:
                logger.debug(f"ContainerConfig not found for sensor: {sensor_id}")
                return None

            return self._to_aggregate(model)

        except Exception as e:
            logger.error(
                f"Error finding container config by sensor ID {sensor_id}: {e}",
                exc_info=True
            )
            raise

    def find_all(self) -> List[ContainerConfig]:
        """
        Find all container configs

        Returns:
            List of ContainerConfig aggregates
        """
        try:
            models = ContainerConfigModel.select()
            return [self._to_aggregate(model) for model in models]

        except Exception as e:
            logger.error(f"Error finding all container configs: {e}", exc_info=True)
            raise

    def delete(self, container_id: str) -> bool:
        """
        Delete container config by ID

        Args:
            container_id: UUID of the container

        Returns:
            True if deleted, False if not found
        """
        try:
            deleted = ContainerConfigModel.delete().where(
                ContainerConfigModel.container_id == container_id
            ).execute()

            if deleted > 0:
                logger.info(f"ContainerConfig deleted: {container_id}")
                return True
            else:
                logger.warning(
                    f"ContainerConfig not found for deletion: {container_id}"
                )
                return False

        except Exception as e:
            logger.error(
                f"Error deleting container config {container_id}: {e}",
                exc_info=True
            )
            raise

    def count(self) -> int:
        """
        Count total container configs

        Returns:
            Total number of configs
        """
        try:
            return ContainerConfigModel.select().count()
        except Exception as e:
            logger.error(f"Error counting container configs: {e}", exc_info=True)
            raise

    def _to_aggregate(self, model: ContainerConfigModel) -> ContainerConfig:
        """
        Convert Peewee model to Domain aggregate

        Args:
            model: ContainerConfigModel instance

        Returns:
            ContainerConfig aggregate
        """
        return ContainerConfig(
            container_id=model.container_id,
            max_fill_level_threshold=model.max_fill_level_threshold,
            sensor_id=model.sensor_id,
            last_sync_at=model.last_sync_at
        )

    def get_latest(self) -> Optional[ContainerConfig]:
        """
        Devuelve el ContainerConfig más reciente ordenando por `last_sync_at`.
        """
        try:
            query = ContainerConfigModel.select().order_by(ContainerConfigModel.last_sync_at.desc()).limit(1)
            model = next(iter(query), None)
            if model is None:
                logger.debug("No se encontró ningún ContainerConfig al solicitar el más reciente")
                return None
            return self._to_aggregate(model)
        except Exception as e:
            logger.error(f"Error obteniendo el ContainerConfig más reciente: {e}", exc_info=True)
            raise


    def get_latest_for_send_threshold(self) -> Optional[Tuple[str, float]]:
        """
        Extrae `(device_id, threshold)` del registro más reciente.
        Prioriza `sensor_id` como identificador de dispositivo; si no existe usa `container_id`.
        Devuelve None si falta información válida.
        """
        latest = self.get_latest()
        if latest is None:
            return None

        device_id = latest.sensor_id or latest.container_id
        if not device_id:
            logger.warning("Último ContainerConfig no tiene sensor_id ni container_id válidos")
            return None

        try:
            threshold = float(latest.max_fill_level_threshold)
        except (TypeError, ValueError):
            logger.warning(f"Umbral inválido en el último ContainerConfig para dispositivo {device_id}")
            return None

        return device_id, threshold