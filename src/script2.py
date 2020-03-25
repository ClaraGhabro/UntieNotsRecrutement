import json
from kafka import KafkaConsumer, KafkaProducer

JSON_PATH = "../topics/topics.json"

if __name__ == "__main__":
    consumer = KafkaConsumer("sendWord", bootstrap_servers="localhost:9092", group_id="groupe1")
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    topics = json.load(open(JSON_PATH, 'r'))

    for message in consumer:
        m = json.loads(message.value.decode())

        belong_to_topic = []

        for elt in topics:
            if m["word"] == elt:
                print("Q3: on a un topic")
                # on envoie dans Q3
                q3_message = { "source": m["source"], "topic": elt}
                producer.send("topicName", json.dumps(q3_message).encode())
            if m["word"] in topics[elt]:
                print("Q2 on a un keyword de topic")
                # on envoie dans Q2
                belong_to_topic.append(elt)

        if len(belong_to_topic) != 0:
            q2_message = {"source": m["source"], "word": m["word"], "topics": belong_to_topic}
            producer.send("topicKeyword", json.dumps(q2_message).encode())
