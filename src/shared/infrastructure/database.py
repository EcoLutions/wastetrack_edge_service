from peewee import SqliteDatabase

# Initialize the SQLite database
db = SqliteDatabase('waste_track.db')

def init_db()->None:
    db.connect()
    from container.infrastructure.models import ContainerRecord
    db.create_tables([ContainerRecord], safe=True)
    db.close()
