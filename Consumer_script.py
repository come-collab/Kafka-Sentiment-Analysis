#We are going to create a streamlit app to visualize the data
import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
# Initialize Kafka Consumer
consumer = KafkaConsumer(

    'bitcoin_reddit',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize an empty list to store data
data_list = []

# Process messages from Kafka
for message in consumer:
    data = message.value
    data_list.append(data)

# Convert list to DataFrame
df = pd.DataFrame(data_list)
print(df)

# Process messages from Kafka

