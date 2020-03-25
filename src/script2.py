import json
from kafka import KafkaConsumer


if __name__ == "__main__":
    consumer = KafkaConsumer("sendWord", bootstrap_servers="localhost:9092", group_id="groupe1")

    onrecoit = []
    print("on a recu des truc peut etre: ", consumer.__sizeof__())
    print("next: ", consumer.next_v1())
    for message in consumer:
        paaire = json.loads(message.value.decode())
        print("pair: ", paaire)
        onrecoit.append(paaire)


    print("len des pair: " + len(onrecoit))