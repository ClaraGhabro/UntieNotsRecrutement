import json
import os
import pyarrow.parquet as pq
import pandas as pd
import sys



JSON_PATH = "../topics/topics.json"

SOURCE_NB = 19


PATH_Q2 = "../output/q2/parquet/"
PATH_Q3 = "../output/q3/parquet/"


def isolate_parquet_file(dir_path):
    """
    Separate parquet files from the other
    Not working because there is a .crc at the end of the file that is strangely managed ...
    :param dir_path: path to the directory containing the files
    """
    data_paths = [os.path.join(pth, f) for pth, dirs, files in os.walk(dir_path) for f in files]
    for file in data_paths:
        if "parquet" in file.__str__():
            split_file_tmp = file.rsplit(".", 1)
            split_file = split_file_tmp[0].rsplit("/", 1)
            new_file_name = split_file[0] + "/parquet/" + split_file[1]
            os.rename(split_file_tmp[0], new_file_name)


def find_source_and_occurence(df):
    return df\
        .groupby(["topic_split", "source", "word"])\
        .size()\
        .to_frame("occurence")\
        .reset_index()


def find_false_positive(x, topic, keywords, df):
    reduce_df = df[df.topic_split == topic]
    if __name__ == '__main__':
        fp = reduce_df\
            .groupby(["source"])\
            .size() \
            .to_frame("nb_occ")\

    fp = fp.nb_occ.map(lambda occ: False if occ / len(keywords) > x else True)

    return fp


def rate_presence_all_source(df, nb_source):
    rate = df\
        .groupby("word")\
        .size()\
        .to_frame("occurence")\
        .reset_index()\
        .drop_duplicates()

    rate["rate"] = rate.occurence.map(lambda occ: occ / nb_source)

    print("rate: ", rate)

    return rate


def rate_presence_source_not_belonging(keyword, df, topic, false_positive):
    return


def rate_absence_source_belonging(keyword, df, topic, false_positive):
    return



def load_parquet(dire_path):
    parquet = pq.read_table(dire_path)
    dico = parquet.to_pydict()
    df = pd.DataFrame.from_dict(dico)

    return df


if __name__ == "__main__":
    x = 0.5
    if len(sys.argv) > 1:
        try:
            x = float(sys.argv[1])
            print("Info: using value", x)
        except ValueError:
            print("Error: invalide argument, using 0.5 as default value.")
        if x < 0 or x > 1:
            raise ValueError("Value must be between 0 and 1")

    else:
        print("Info: no argument given, using 0.5 as default value.")

    topics = json.load(open(JSON_PATH, 'r'))
    print(topics)

    load_q3 = load_parquet(PATH_Q3)

    df_q3 = pd.DataFrame(columns=["source", "topic"])
    df_q3["source"] = load_q3.value.apply(lambda x: json.loads(x)["source"])
    df_q3["topic"] = load_q3.value.apply(lambda x: json.loads(x)["topic"])

    parquet_q3 = pq.read_table(PATH_Q3)
    par_q3 = parquet_q3.to_pandas()

    load_q2 = load_parquet(PATH_Q2)

    df_q2 = pd.DataFrame(columns=["source", "word", "topics"])
    df_q2["source"] = load_q2.value.apply(lambda x: json.loads(x)["source"])
    df_q2["word"] = load_q2.value.apply(lambda x: json.loads(x)["word"])
    df_q2["topics"] = load_q2.value.apply(lambda x: json.loads(x)["topics"])

    df_q2["topic_split"] = df_q2.topics.apply(lambda x: " ".join(x))

    occurence_per_source_q2 = find_source_and_occurence(df_q2)
    print("occurence: ", occurence_per_source_q2)

    support_fp = {}
    for topic in topics:
        support_fp[topic] = len(topics[topic])

    false_positive = {}
    for topic in topics:
        false_positive_q2 = find_false_positive(x, topic, topics[topic], occurence_per_source_q2)
        false_positive[topic] = false_positive_q2

    # print(false_positive)
    rate_presence_belonging = {}
    rate_presence_not_belonging = {}
    rate_absence_belonging = {}

    
    presence_belonging = rate_presence_all_source(occurence_per_source_q2, SOURCE_NB)


