from datetime import timezone, datetime
from container.domain.entities import ContainerRecord


class ContainerRecordService:
    def __init__(self):
        pass

    @staticmethod
    def create_record(device_id: str, fill_level_percentage: float)-> ContainerRecord:
        try:
            fill_level_percentage = float(fill_level_percentage)
            if not (0 <= fill_level_percentage <= 100):
                raise ValueError("Container/domain/services: Invalid fill_level_percentage value")
            created_at = datetime.now(timezone.utc)
        except (ValueError, TypeError):
            raise ValueError("Container/domain/services: Invalid data format")

        return ContainerRecord(device_id, fill_level_percentage, created_at)
