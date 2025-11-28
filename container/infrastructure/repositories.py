from container.domain.entities import ContainerRecord
from container.infrastructure.models import ContainerRecord as ContainerRecordModel

class ContainerRecordRepository:
    @staticmethod
    def save(container_record)-> ContainerRecord:
        record = ContainerRecordModel.create(
            device_id=container_record.device_id,
            fill_level_percentage=container_record.fill_level_percentage,
            created_at=container_record.created_at
        )
        return ContainerRecord(
            device_id=container_record.device_id,
            fill_level_percentage=container_record.fill_level_percentage,
            created_at=container_record.created_at,
            id=record.id
        )