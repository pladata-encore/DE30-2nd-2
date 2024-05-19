from spotify.spotify_api import call_playlist
from kafka_fn.kafka_function import create_topic, delete_topics, produce_to_topic
from spark_fn.kafka_to_spark import makeSchema, spotify_KafkaToParquet
from flask import Flask
from flask_apscheduler import APScheduler

topic_name = 'spotify_data'
create_topic(topic_name)

class Config:
    SCHEDULER_API_ENABLED = True

app = Flask(__name__)
app.config.from_object(Config())

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

@app.route('/')
def index():
    return "Flask Server!"

@scheduler.task('cron', id='schedule_1', second=5)
def schedule_1():
    pid = '37i9dQZEVXbNxXF4SkHj9F'
    country = 'KR'
    df = call_playlist(pid, country)
    data = df.to_dict(orient='records')
    topic_name = 'spotify_data'
    produce_to_topic(data, topic_name)

@scheduler.task('cron', id='schedule_2', second=55)
def schedule_2():
    kafka_topic = "topic_name"
    output_path = "hdfs://localhost:9000/user/hadoop/output"
    schema = makeSchema()
    spotify_KafkaToParquet(kafka_topic, output_path, schema)
    
if __name__ == '__main__':
    app.run()