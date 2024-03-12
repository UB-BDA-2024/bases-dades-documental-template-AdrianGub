from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.mongodb_client import MongoDBClient
from app.redis_client import RedisClient
from . import models, schemas
import json
from typing import Dict

# Get sensor by id
def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

# Get sensor by name
def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

# Get all sensors
def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

# Create a sensor
def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    
    # Insert sensor info
    mongodb_client.getDatabase("mydatabase")
    mongo_info = sensor.dict()
    mongodb_client.getCollection("sensors").insert_one(mongo_info)
    return db_sensor

# Record sensor data
def record_data(redis: Session, sensor_id: int, data: schemas.SensorData, mongodb_client: MongoDBClient, sensor_name: str) -> schemas.Sensor:
    # Save dynamic data in Redis
    redis_data = data.json()
    redis.set(sensor_id, redis_data)
    
    # Get sensor data from Mongo and pop _id
    mongodb_client.getDatabase("mydatabase")
    mongo_info = mongodb_client.getCollection("sensors").find_one({'name': sensor_name})
    redis_dict = json.loads(redis_data)
    mongo_info.pop('_id', None)
    
    # Merge Redis data and Mongo data to get the full sensor data
    merged_data = {**redis_dict, **mongo_info}
    sensor_data = json.dumps(merged_data)
    return sensor_data

# Get sensor data
def get_data(redis: Session, sensor_id: int, mongodb_client: MongoDBClient, sensor_name: str) -> schemas.Sensor:
    # Get Redis data
    redis_data = redis.get(sensor_id)
    
    # Get Mongo data and pop _id
    mongodb_client.getDatabase("mydatabase")
    mongo_info = mongodb_client.getCollection("sensors").find_one({'name': sensor_name})
    redis_dict = json.loads(redis_data)
    mongo_info.pop('_id', None)
    
    # Merge Redis data and Mongo data to get the full sensor data
    merged_data = {**redis_dict, **mongo_info}
    
    # Add the real id of the sensor
    merged_data['id'] = sensor_id
    return merged_data

# Delete a sensor
def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

# Get sensors near to a latitude and longitud given a radius
def get_sensors_near(mongodb_client: MongoDBClient, db: Session, redis_client: Session, latitude: float, longitude: float, radius: float):
    db_sensors = get_sensors(db=db)
    near_sensors = []
    # For each sensor, get data, specifically latitud and longitude
    for sensor in db_sensors:
        sensor_data = get_data(redis=redis_client, sensor_id=sensor.id, mongodb_client=mongodb_client, sensor_name=sensor.name)
        sensor_latitude = sensor_data['latitude']
        sensor_longitude = sensor_data['longitude']
        # If the latitud and longitude are inside the range, add the sensor to a list
        if (latitude - radius) <= sensor_latitude <= (latitude + radius):
            if (longitude - radius) <= sensor_longitude <= (longitude + radius):
                near_sensors.append(sensor_data)    
    return near_sensors