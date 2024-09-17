import streamlit as st
from kafka import KafkaConsumer
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import pandas as pd
from datetime import datetime
import time

# Download VADER lexicon (only needed once)
nltk.download('vader_lexicon')

# Initialize or load the DataFrame in session state
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame(columns=['timestamp', 'sentiment'])

# Title of the app
st.title('Real-Time Bitcoin Sentiment Analysis')

# Placeholder for the chart
chart_placeholder = st.empty()

# Function to create a Kafka consumer (singleton)
@st.experimental_singleton
def get_kafka_consumer():
    return KafkaConsumer(
        'bitcoin_reddit',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        consumer_timeout_ms=1000,  # Stop after 1 second if no messages
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

# Function to perform sentiment analysis
def analyze_sentiment(data):
    sid = SentimentIntensityAnalyzer()
    text = data.get('body', '')
    sentiment = sid.polarity_scores(text)
    timestamp = datetime.fromtimestamp(data.get('created_utc', datetime.now().timestamp()))
    compound_score = sentiment['compound']
    return {'timestamp': timestamp, 'sentiment': compound_score}

# Fetch new data from Kafka
def fetch_new_data():
    consumer = get_kafka_consumer()
    new_data = []

    for message in consumer:
        data = message.value
        result = analyze_sentiment(data)
        new_data.append(result)

    return new_data

# Fetch and process new data
new_data = fetch_new_data()
if new_data:
    new_df = pd.DataFrame(new_data)
    st.session_state.df = pd.concat([st.session_state.df, new_df], ignore_index=True)

# Process and display the data
if not st.session_state.df.empty:
    df = st.session_state.df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    df.sort_index(inplace=True)

    # Resample and calculate mean sentiment
    sentiment_over_time = df['sentiment'].resample('T').mean()  # Resample per minute

    # Update the chart
    chart_placeholder.line_chart(sentiment_over_time)

# Auto-refresh the app every 5 seconds
st.experimental_autorefresh(interval=5000, key="auto_refresh")