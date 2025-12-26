#Import requirements
import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

#Load environment variables
load_dotenv()


#Define variables for API
API_KEY=os.getenv("API_KEY_HONNUB")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-quotes")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

#Initial Producer
producer = KafkaProducer (
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#Retrive Data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int (time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

#Looping and Pushing to Stream
while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing: {quote}")
            producer.send(KAFKA_TOPIC, value=quote)
    time.sleep(6)
