import json
from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime


def consume_message(consumer, queue):
    for message in consumer:
        print("on a recu chez q3")
        m = json.loads(message.value.decode())
        df = pd.DataFrame(m, index=[0])

        table = pa.Table.from_pandas(df).replace_schema_metadata()

        ts = datetime.datetime.now().timestamp()
        file_name = "../output/" + queue + "/" + ts.__str__() + ".parquet"

        pqwriter = pq.ParquetWriter(file_name, schema=table.schema)

        pqwriter.write_table(table)


if __name__ == "__main__":
    consumer_Q3 = KafkaConsumer("topicName", bootstrap_servers="localhost:9092", group_id="groupe3")
    consume_message(consumer_Q3, "q3")