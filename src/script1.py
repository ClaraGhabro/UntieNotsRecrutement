import json
from kafka import KafkaProducer
import os


CHAR_TO_REMOVE = ['.', ',', '(', ')', '"', "'",'!', '?', ';', ':', '«', '»', '\n']

def clean_line(line):
    """
    Remove useless char
    :param line: Sentece split by space
    :return line: without ponctuation char
    """
    for c in CHAR_TO_REMOVE:
        line = line.replace(c, " ")
    return line

def remove_empty(words):
    """
    Remove empty words
    :param words:
    :return:
    """
    return list(filter(("").__ne__, words))

def read_corpus(dir_path):
    """
    Read all the files contained in the given path, split text in words.
    :param dir_path:
    :return:
    """
    data_paths = [os.path.join(pth, f) for pth, dirs, files in os.walk(dir_path) for f in files]
    words_dico = {}
    for p in data_paths:
        f = open(p, "r")
        lines = f.readlines()
        words_list = []
        for line in lines:
            line = clean_line(line)
            words = remove_empty(line.split(" "))
            words_list.append(words)

        flat_list = [w for word in words_list for w in word]
        words_dico[p] = flat_list

    return words_dico

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    data = read_corpus("../corpus/")
    for file_name, words in data.items():
        for w in words:
            d = {"source": file_name, "word": w}
            producer.send("sendWord", json.dumps(d).encode())
