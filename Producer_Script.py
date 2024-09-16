import praw
from kafka import KafkaProducer
import json
from kafka.vendor import six
from dotenv import load_dotenv
import os

load_dotenv()
api_key = os.getenv("API_KEY")
user_name = os.getenv("USER_NAME")
#Setting up the Reddit API
reddit = praw.Reddit(
    client_id='vcvPFQWZnZ5boS_GdaWfdg',
    client_secret=api_key,
    user_agent='my-app by {user_name}'
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Fetch real-time data
subreddit = reddit.subreddit('Bitcoin')
for comment in subreddit.stream.comments(skip_existing=True):
    data = {
        'id': comment.id,
        'body': comment.body,
        'created_utc': comment.created_utc
    }
    producer.send('bitcoin_reddit', value=data)
    print(f"Produced comment ID {comment.id}")