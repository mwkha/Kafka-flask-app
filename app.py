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
    """Returning home page for restaurant"""
    if request.method == 'POST':
        producer.send(
            TOPIC_ORDER_NAME,
            key={"caller": request.form.get("caller")},
            value={
                "caller": request.form.get("caller"),
                "restaurant": request.form.get("restaurant"),
                "address": request.form.get("address"),
                "timestamp": int(round(time.time() * 1000))
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

@app.route('/restaurant-orders')
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
                message.value["restaurant"],
                message.value["address"],
                1]

    return Response(stream_template('restaurant-orders.html', data=consume_msg()))

@app.route('/order-ready/<my_id>', methods=['POST'])
def pizza_ready(my_id=None):
    """Endpoint to pass ready orders"""
    print(my_id)
    producer.send(
        TOPIC_DELIVERY_NAME,
        key={"timestamp": my_id},
        value=request.json
    )
    producer.flush()
    return "OK"

@app.route('/order-delivery-pickup')
def consume_delivery():
    """Returning home page for Pizza delivery"""
    consumer_delivery = KafkaConsumer(
        client_id="clientDelivery",
        group_id=CONSUMER_DELIVERY_GROUP,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda v: json.loads(v.decode('ascii')),
        key_deserializer=lambda v: json.loads(v.decode('ascii')),
        max_poll_records=10,
        auto_offset_reset='earliest',
        session_timeout_ms=6000,
        heartbeat_interval_ms=3000
    )
    consumer_delivery.subscribe(topics=[TOPIC_DELIVERY_NAME])

    def consume_msg_delivery():
        for message in consumer_delivery:
            print(message.value)
            yield [
                message.key["timestamp"],
                message.value["caller"],
                message.value["address"]
            ]

    return Response(
        stream_template('order-delivery-pickup.html', data=consume_msg_delivery()))

if __name__ == "__main__":
    app.run(debug=True, port=5000)
