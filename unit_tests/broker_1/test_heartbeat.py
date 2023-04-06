import requests
import json
import sys

try:
    port = sys.argv[1]
except:
    port = input("Port: ")

broker_url = f"http://127.0.0.1:{port}"

resp = requests.get(broker_url)
print(25*"-")
print("Requesting Heartbeat")
print(f"Status Code = {resp.status_code}")
print(resp.json())
print(25*"-")


