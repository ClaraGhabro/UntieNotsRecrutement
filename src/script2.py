import os
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars resources/spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer


JSON_PATH = "../topics/topics.json"


def udf_process(rdd):
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    for line in rdd:
        message = json.loads(line[1])

        belong_to_topic = []
        for elt in topics:
            if message["word"] == elt:
                # on a un nom de topic => on envoie dans Q3
                q3_message = {"source": message["source"], "topic": elt}
                producer.send("topicName", json.dumps(q3_message).encode())
            if message["word"] in topics[elt]:
                # on a un keyword de topic : on stock le topic
                belong_to_topic.append(elt)

        if len(belong_to_topic) != 0:
            q2_message = {"source": message["source"], "word": message["word"], "topics": belong_to_topic}
            producer.send("topicKeyword", json.dumps(q2_message).encode())

if __name__ == "__main__":
    # Creation du Stream
    sc = SparkContext(appName='PythonStreamingRecieverKafka')
    ssc = StreamingContext(sc, 2)  # 2 second window
    zookeeper_broker = "localhost:2181"
    topic = "sendWord"

    topics = json.load(open(JSON_PATH, 'r'))
    kafkaStream = KafkaUtils.createStream(groupId="group1", ssc=ssc, topics={topic: 1}, zkQuorum=zookeeper_broker)

    kafkaStream.foreachRDD(lambda x: x.foreachPartition(lambda message: udf_process(message)))

    ssc.start()
    ssc.awaitTermination()
