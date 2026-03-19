import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
import functools
print = functools.partial(print, flush=True)

# Retry connecting to Kafka up to 10 times with 5 second gaps
# This handles the case where Kafka isn't ready yet when the producer starts
def create_producer():
    for attempt in range(10):
        try:
            print(f"Connecting to Kafka... attempt {attempt + 1}")
            return KafkaProducer(
                bootstrap_servers=os.environ.get('KAFKA_BROKER', 'localhost:9092'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except NoBrokersAvailable:
            print("Kafka not ready yet — waiting 5 seconds...")
            time.sleep(5)
    raise Exception("Could not connect to Kafka after 10 attempts")

producer = create_producer()

def fetch_flights():
    """Call the OpenSky API and return raw flight states"""
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"API returned status {response.status_code} — skipping this fetch")
            return []
        data = response.json()
        return data.get('states', [])
    except Exception as e:
        print(f"Error fetching flights: {e}")
        return []

def parse_flight(state):
    """Convert raw API array into a readable dictionary"""
    return {
        "icao24":         state[0],   # Unique aircraft transponder ID
        "callsign":       state[1],   # Flight number e.g. UAL123
        "origin_country": state[2],   # Country of registration
        "longitude":      state[5],   # Current longitude
        "latitude":       state[6],   # Current latitude
        "altitude":       state[7],   # Altitude in meters
        "velocity":       state[9],   # Ground speed in m/s
        "heading":        state[10],  # Direction in degrees
        "on_ground":      state[8],   # True if aircraft is on the ground
        "timestamp":      state[3],   # Unix timestamp of last position update
        "category":       state[17] if len(state) > 17 else None,
    }

print("Producer starting — fetching flights every 10 seconds...")

# Main loop — runs forever until the container is stopped
while True:
    flights = fetch_flights()
    print(f"Fetched {len(flights)} flights")

    for state in flights:
        # Only send flights that have valid coordinates
        if state[5] and state[6]:
            flight = parse_flight(state)
            producer.send('flights-raw', flight)

    producer.flush()
    print(f"Sent {len(flights)} flights to Kafka topic: flights-raw")
    time.sleep(10)