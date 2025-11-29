from peewee import Model, AutoField, CharField, FloatField, DateTimeField
from src.shared.infrastructure.database import db


class ContainerRecord(Model):
    id = AutoField()
    device_id = CharField()
    fill_level_percentage = FloatField()
    created_at = DateTimeField()

    class Meta:
        database = db
        table_name = 'container_records'