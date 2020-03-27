# UntieNotsRecrutement

### Dépendances :

python 3.6  
kafka-python 2.0.1  
pyarrow 0.16.0  
pandas 1.0.3  
pyspark 2.4.5  


### Mise en place de l'environnement kafka

Lancer les commandes suivantes :  
    ./bin/zookeeper-server-start.sh ./config/zookeeper.properties  
    ./bin/kafka-server-start.sh ./config/server.properties  

Création des topics :  
    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sendWord  
    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topicKeyword  
    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topicName  

**sendWord**     correspond au topic **Q1**  
**topicKeyword** correspond au topic **Q2**  
**topicName**    correspond au topic **Q3**  


### 1ère partie

Lecture de l'ensemble des textes contenus dans le dossier **corpus**  
Tokenization des textes manuellement.  
Envoie de chaque mot dans le topic kafka **sendWord (Q1)** au format **{"source": <file_name>, "word": <my_word>}**  

### 2ème partie

Chargement d'une liste de topics depuis le fichier json topics/topics.json  
Reception des messages du topic **Q1** via un consumer kafka  
Analyse du mot reçu au regard des topics.  
Si le mot appartient à un topic, on envoie le message au format **{"source": <file_name>, "word": <word>, "topics": [<topics>]}** dans le topic **topicKeyword (Q2)**  
Si le mot correspond au nom d'un topic, on envoie le message au format **{"source": <file_name>, "topic": <topic>}** dans le topic **topicName (Q3)**  

### 3ème partie

Le script script3_q2.py receptionne les messages envoyés dans le topic **Q2** et les sauvegarde au format parquet dans le dossier **output/q2**  
Le script script3_q3.py receptionne les messages envoyés dans le topic **Q3** et les sauvegarde au format parquet dans le dossier **output/q3**  

### 4ème partie

Lecture des fichiers dans **output/q2** et **output/q3**, et creation des tables associées.  
Chargement en mémoire des topics contenus dans le fichier **topics.topics.json**  
Pour chaque topic, on recupère les sources associées ainsi que le nombre d'occurence de chacun des mot appartenant au topic et présents dans le document  
On calcule ensuite les faux positifs.  
En derniere, on calcule la pertinence de chacun des keyword du topic. (Pas encore effectué au moment du rendu, sera probablement fait durant le weekend)  


### Remarques

- Lors de l'execution du script 3, il y a 3 fichiers .parquet qui sont générés, et qui semblent dupliquer les données par rapport au corpus initial. J'ai n'ai pas vraiment eu le temps d'investiguer.  
- Les topics dans le fichiers json gagneraient a être enrichis pour qu'il y ai beaucoup moins de faux positifs lors de l'analyse par le script 4.
