from kafka import KafkaConsumer
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
from pymongo import MongoClient
# Download VADER lexicon
nltk.download('vader_lexicon')

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'bitcoin_reddit',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Sentiment Analyzer
sid = SentimentIntensityAnalyzer()

# Initialize MongoDB client
client = MongoClient('localhost', 27017)
db = client['reddit']
collection = db['bitcoin_comments']

for message in consumer:
    data = message.value
    text = data['body']
    sentiment = sid.polarity_scores(text)
    data['sentiment'] = sentiment
    collection.insert_one(data)
    print(f"Stored comment ID {data['id']} with sentiment {sentiment}")