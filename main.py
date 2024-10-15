from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
import sys

import json

load_dotenv()

import time
import os
from apis.service import Service
from scripts.geolocation import GeoLocation


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "name": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        return json.dumps(log_record)


def setup_logger(name):
    logger = logging.getLogger(name)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(JsonFormatter())
    logger.addHandler(stream_handler)

    file_handler = RotatingFileHandler(f"{name}.log", maxBytes=2000, backupCount=5)
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)

    logger.setLevel(logging.DEBUG)
    return logger


app_logger = setup_logger("app")


def format_worker_log(worker_data):
    data = worker_data["data"]
    status = worker_data["status"]

    log_message = (
        f"Worker ID: {data['id']}\n"
        f"Name: {data['name']}\n"
        f"IP Address: {data['ip_address']}\n"
        f"Provider: {data['provider'] or 'N/A'}\n"
        f"City: {data['city']}\n"
        f"Region: {data['region']}\n"
        f"Country: {data['country']}\n"
    )

    return log_message


class Main:
    def __init__(self, token, service=None):
        self.token = token
        self.service = service
        self.version = None
        self.is_running = False
        self.worker = None

    def get_worker(self):
        return self.service.get_worker(user_id=1)

    def start_collection(self):
        if not self.worker:
            self.worker = self.service.get_worker(user_id=1)
        if not self.version and self.worker:
            self.version = self.service.version(worker_id=self.worker["data"]["id"])

        if not self.is_running and self.worker:
            try:
                self.handle_version = self.service.handle_version(
                    worker_id=self.worker["data"]["id"],
                    version_id=self.version["data"]["id"],
                )
                self.is_running = True
                self.run_script()
                self.is_running = False
            except Exception as e:
                app_logger.error(
                    f"An error occurred while starting the collection: {e}"
                )
        else:
            app_logger.error("No worker or collection already running")

    def create_worker(self, worker_name, geolocation_info):
        if not self.service:
            self.service = Service()

        try:
            app_logger.info("Creating worker")
            self.worker = self.service.post_worker(
                worker_name=worker_name,
                user_id=1,
                ip_address=geolocation_info["ip"],
                city=geolocation_info["city"],
                region=geolocation_info["region"],
                country=geolocation_info["country"],
            )
            app_logger.info("Success: Worker created successfully")
        except Exception as e:
            app_logger.error(f"An error occurred while creating worker: {e}")

    def update_worker(self, worker_name, geolocation_info):
        if not self.service:
            self.service = Service()

        try:
            self.service.put_worker(
                worker_id=self.worker["data"]["id"],
                worker_name=worker_name,
                ip_address=geolocation_info["ip"],
                city=geolocation_info["city"],
                region=geolocation_info["region"],
                country=geolocation_info["country"],
            )
            app_logger.info("Success: Worker updated successfully")
        except Exception as e:
            app_logger.error(f"An error occurred while updating worker: {e}")

    def run_script(self):
        app_logger.info(
            f"Running script on range: {self.handle_version['data']['rank_start']} - {self.handle_version['data']['rank_end']}"
        )
        os.system(
            f"python3 scripts/runner.py {self.handle_version['data']['id']} {self.version['data']['id']} {self.handle_version['data']['rank_start']} {self.handle_version['data']['rank_end']}"
        )


if __name__ == "__main__":

    geolocation = GeoLocation()
    geolocation_info = geolocation.get_location()
    app_logger.info(f"Running on IP Address: {geolocation_info['ip']}")

    service = Service()

    session = service.login(
        username=os.getenv("USERNAME"), password=os.getenv("PASSWORD")
    )

    if session:
        main = Main(token=session["access"], service=service)
    else:
        app_logger.error("Login failed")
        exit(1)

    worker = main.get_worker()
    print(format_worker_log(worker))

    counter = 0

    if worker:
        time.sleep(5)
        app_logger.info("Starting collection")
        while counter <= 5:
            result = main.start_collection()
            counter += 1
    else:
        app_logger.error("Worker not found")
        main.create_worker(
            worker_name=os.getenv("WORKER_NAME"), geolocation_info=geolocation_info
        )
