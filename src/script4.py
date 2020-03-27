import json
import pyarrow.parquet as pq
import pandas as pd

JSON_PATH = "../topics/topics.json"


PATH_Q2 = "../output/q2"
PATH_Q3 = "../output/q3"

def find_source_and_occurence(df):
    return df.groupby(["topic", "word", "source"]).size().to_frame("occurence").reset_index()

def find_false_positive(x, topic, df):
    fp = df.groupby("source", "topic", "word")(lambda tp: tp == topic)

    print(fp)
    return df

if __name__ == "__main__":
    topics = json.load(open(JSON_PATH, 'r'))

    parquet_q2 = pq.read_table(PATH_Q2)
    df_q2 = parquet_q2.to_pandas()
    df_q2["topic"] = df_q2.topics.apply(lambda x: "".join(x))

    parquet_q3 = pq.read_table(PATH_Q3)
    par_q3 = parquet_q3.to_pandas()

    occurence_per_source_q2 = find_source_and_occurence(df_q2)
    print(occurence_per_source_q2)
    # topic: "", keyword: "", source: "", occurence: ""
    false_positive = {}
    for topic in topics:
        false_positive_q2 = find_false_positive(80, topics[topic], occurence_per_source_q2)



    #for elt in par:
    #    print("elt: ", elt)
