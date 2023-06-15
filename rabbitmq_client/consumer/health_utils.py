import json
import os

HEALTH_STATUS_DIR = "/tmp/consumer/health_status"
os.makedirs(HEALTH_STATUS_DIR, exist_ok=True)


def set_consumer_status(is_healthy: bool):
    health_status = {"is_healthy": is_healthy}
    with open(os.path.join(HEALTH_STATUS_DIR, "status.json"), "w+") as f:
        json.dump(health_status, f)
