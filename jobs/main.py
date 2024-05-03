import os
import random
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime

LONDON_COORDIMATES = { "latitude": 51.5074, "longitude" -0.1278}
BIRMINGHAM_COORDIMATES = { "latitude": 52.4862, "longitude" -1.8904}

#CALCULATE movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDIMATES['latitude'] - LONDON_COORDIMATES['latitude'])/100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDIMATES['longitude']- LONDON_COORDIMATES['longitude'])/100

#Environment Variable for configuration
KAFKA_BOOTSTRAP_SERVERS= os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC= os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDIMATES.copy()

def simulate_vehicle_movement():
    global start_location
    #move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    #add some randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()


def simulate_journey(produver, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)

if __name__ == "__main__"
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-CodeWithYu-123')
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected Error occurred: {e}')