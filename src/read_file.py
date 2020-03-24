import os


CHAR_TO_REMOVE = ['.', ',', '(', ')', '"', '!', '?', ';', ':', '\n']

def clean_line(line):
    """
    Remove useless char
    """
    for c in CHAR_TO_REMOVE:
        line = line.replace(c, "")
    return line

def read_corpus(dir_path):
    data_paths = [os.path.join(pth, f) for pth, dirs, files in os.walk(dir_path) for f in files]
    words_dico = {}
    for p in data_paths:
        f = open(p, "r")
        lines = f.readlines()
        words_list = []
        for line in lines:
            line = clean_line(line)
            words = line.split(" ")
            words_list.append(words)
        words_dico[p] = words_list

    print(words_dico["../corpus/Bohort.txt"])



read_corpus("../corpus/")