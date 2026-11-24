import random
from datetime import datetime, timezone
import boto3
import json

def lambda_handler(event, context):
    sqs = boto3.client("sqs")

    response = sqs.get_queue_url(
        QueueName="coldchain-sensor-raw-queue"
    )

    queue_url = response["QueueUrl"]

    warehouses = ["WH-01", "WH-02", "WH-03"]
    zones = ["cold-storage-A", "cold-storage-B", "dry-storage", "loading-dock"]
    reading_types = ["temperature", "humidity"]

    num_readings = random.randint(3, 5)

    warehouse_counts = {
        "WH-01": 0,
        "WH-02": 0,
        "WH-03": 0
    }

    zone_counts = {
        "cold-storage-A": 0,
        "cold-storage-B": 0,
        "dry-storage": 0,
        "loading-dock": 0
    }

    total_published = 0

    for i in range(num_readings):
        sensor = {
            "warehouse_id": random.choice(warehouses),
            "zone": random.choice(zones),
            "reading_type": random.choice(reading_types)
        }
        
        if sensor["reading_type"] == "humidity":
            if random.random() < 0.1:
                value = round(random.uniform(65, 80), 1)
            else:
                value = round(random.uniform(30, 60),1)
        elif sensor["reading_type"] == "temperature" and sensor["zone"] in ["cold-storage-A", "cold-storage-B"]:
            if random.random() < 0.15:
                value = round(random.uniform(-10, 0),1)
            else:
                value = round(random.uniform(-25, -15),1)
        elif sensor["reading_type"] == "temperature" and sensor["zone"] in ["dry-storage", "loading-dock"]:
            if random.random() < 0.15:
                value = round(random.uniform(30, 50),1)
            else:
                value = round(random.uniform(15, 25),1)

        sensor["value"] = value
        
        if sensor["reading_type"] == "temperature":
            unit = "celsius"
        else:
            unit = "percent"

        sensor["unit"] = unit
        
        sensor["battery_pct"] = random.randint(5, 100)

        if sensor["battery_pct"] < 15:
            print("Low Battery")

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        sensor["timestamp"] = timestamp
        
        warehouse_id = sensor["warehouse_id"]
        warehouse = warehouse_id.replace("-", "")
        number = random.randint(1, 20)

        if sensor["reading_type"] == "temperature":
            reading_code = "T"
        else:
            reading_code = "H"

        sensor_id = f"SENS-{warehouse}-{reading_code}{number:03d}"

        sensor["sensor_id"] = sensor_id

        message_body = json.dumps(sensor)

        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )

        warehouse_counts[sensor["warehouse_id"]] += 1
        zone_counts[sensor["zone"]] += 1
        total_published += 1

        message_id = response["MessageId"]
        print("Message sent successfully:", message_id)

    summary = {
        "total_published": total_published,
        "warehouse_counts": warehouse_counts,
        "zone_counts": zone_counts
    }

    return summary

if __name__ == "__main__":
    result = lambda_handler({}, None)
    print("Lambda summary:", result)
