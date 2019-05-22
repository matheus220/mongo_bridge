from mongoengine import *

class Cycle(DynamicDocument):
    cycle_id = FloatField()
    start_time = StringField()
    end_time = StringField()
    mode = StringField()
    status = StringField()