import json
import dns.resolver
import concurrent.futures
import sys
from datetime import datetime
import requests
import os
import glob

from tqdm import tqdm

from crawler import CrawlerThread
from file_writer import FileWriter

BASE_URL = os.getenv("BASE_URL")


def process_domain(domain, processedDomain, crawler, query_objects):
    if domain not in processedDomain:
        processedDomain.add(domain)
        result = crawler.start(domain)
        for obj in result:
            query_objects.append(obj)
    return domain


if __name__ == "__main__":

    base_dir = os.path.dirname(os.path.abspath(__file__))

    handle_id = sys.argv[1]
    version_id = sys.argv[2]
    range_start = int(sys.argv[3])
    range_end = int(sys.argv[4])

    uncompleted_file = glob.glob(
        f"avro_files/collections/version_{version_id}_handle_{handle_id}_*.parquet"
    )

    if len(uncompleted_file) > 0:
        print("There is an uncompleted file, collection again.")
        for file in uncompleted_file:
            os.remove(file)

    query_file_name = "version_{}_handle_{}_{}.parquet".format(
        version_id, handle_id, datetime.now()
    )

    today = datetime.now()
    domainList = []
    processedDomain = set()
    domainErrorDict = set()

    query_objects = []

    list_file = "lists/top-1m.txt"

    crawler = CrawlerThread(handle_id=handle_id, query_file_name=query_file_name)

    with open(list_file, "r") as f:
        lines = f.readlines()[range_start:range_end]
        for k in lines:
            sp = k.split(",")
            domain = sp[1].strip()
            domainList.append(domain)

    processedDomain = set()
    query_objects = []

    bar = tqdm(total=len(domainList))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                process_domain, domain, processedDomain, crawler, query_objects
            )
            for domain in domainList
        ]

        for future in concurrent.futures.as_completed(futures):
            bar.update()

    bar.close()

    file_writer = FileWriter("../avro_files/collections/" + query_file_name)

    file_writer.write_parquet(query_objects)

    try:
        data = {"version_id": version_id, "handle_id": handle_id}
        files = {"query_file": open("avro_files/collections/" + query_file_name, "rb")}
        query_url = BASE_URL + "/core/api/query_file/"
        response = requests.post(query_url, files=files, data=data)
        if response.status_code == 200:
            print("\nQuery file successfully uploaded")
            os.remove(os.path.join("avro_files/collections/", query_file_name))
    except Exception as e:
        print(e)
