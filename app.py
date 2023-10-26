# Flask app for pseudo uber eats delivery

import json
import time
from flask import Flask, request, render_template, Response
from kafka import KafkaConsumer, KafkaProducer

HOST = "localhost"
PORT = "9092"
TOPIC_ORDER_NAME = "food-orders"
TOPIC_DELIVERY_NAME = "food-delivery"   
KAFKA_SERVER = HOST + ":" + PORT
CONSUMER_ORDER_GROUP = "food-consumers"
CONSUMER_DELIVERY_GROUP = "food-delivery"
CONSUMER_CALC_GROUP = "food-calcs"


producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    key_serializer = lambda v: json.dumps(v).encode("ascii"),
    value_serializer = lambda v: json.dumps(v).encode("ascii")
    )

app = Flask(__name__, template_folder='templates')

@app.route('/', methods=['GET', 'POST'])
def index():
    """Returning home page for Restraunt"""
    if request.method == 'POST':
        producer.send(
            TOPIC_ORDER_NAME,
            key={"caller": request.form.get("caller")},
            value={
                "caller": request.form.get("caller"),
                "restraunt": request.form.get("restraunt"),
                "address": request.form.get("address"),
                "timestamp": int(time.time())
            }
        )

        producer.flush()
    elif request.method == 'GET':
        return render_template('index.html', form=request.form)

    return render_template("index.html")

def stream_template(template_name, **context):
    """Enabling streaming back results to app"""
    app.update_template_context(context)
    template = app.jinja_env.get_template(template_name)
    streaming = template.stream(context)
    return streaming

@app.route('/restraunt-orders')
def consume():
    """Returning pizza orders"""
    consumer = KafkaConsumer(
        client_id="client1",
        group_id=CONSUMER_ORDER_GROUP,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode('ascii')),
        key_deserializer=lambda v: json.loads(v.decode('ascii')),
        max_poll_records=10,
        auto_offset_reset='earliest',
        session_timeout_ms=6000,
        heartbeat_interval_ms=3000
    )
    consumer.subscribe(topics=[TOPIC_ORDER_NAME])

    def consume_msg():
        for message in consumer:
            print(message.value)
            yield [
                message.value["timestamp"],
                message.value["caller"],
                message.value["restraunt"],
                message.value["address"],
                1]

    return Response(stream_template('restraunt-orders.html', data=consume_msg()))

if __name__ == "__main__":
    app.run(debug=True, port=5000)
    