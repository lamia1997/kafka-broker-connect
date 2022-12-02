import json
import time
import numpy as np
from json import dumps
from kafka import KafkaProducer
import psycopg2

producer = KafkaProducer(bootstrap_servers="localhost:9092",value_serializer=lambda m: dumps(m).encode('utf-8'))
topic1='rayane'
topic2='quickstart'

# Connect to an existing database
conn = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % ("localhost", "kafkaproject", "postgres", "1997"))
cur = conn.cursor()
sql = "SELECT * FROM worldcuplayer;"
cur.execute(sql)
res = cur.fetchall()
for i in range(len(res)):
    producer.send(topic2,value=res[i])
    
