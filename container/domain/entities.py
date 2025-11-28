from datetime import datetime

class ContainerRecord:
    def __init__(self, device_id: str, fill_level_percentage: float, created_at: datetime, id: int = None ):
        self.id = id
        self.device_id = device_id
        self.fill_level_percentage = fill_level_percentage
        self.created_at = created_at