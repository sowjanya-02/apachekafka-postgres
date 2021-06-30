#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 14:46:41 2021

@author: sowjanyagunturu
"""

from kafka import KafkaProducer, KafkaConsumer
import json
import psycopg2
from kafka.admin import KafkaAdminClient, NewTopic

'''
  kafka cluster class with producer and consumer. I would recommend to have producer and consumer in seperate classes as client 
  needs just the producer, and for easy maintainability.
  # reference1: https://github.com/aiven/aiven-examples/blob/master/kafka/python/producer_example.py
  # reference2: https://python.plainenglish.io/how-to-programmatically-create-topics-in-kafka-using-python-d8a22590ecde
'''
class kafka_cluster:
    def __init__(self, server, topic):
        self.server = server
        self.topic = topic
        ### initiating producer
        self.producer = KafkaProducer(
                        bootstrap_servers=self.server,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        key_serializer=lambda v: json.dumps(v).encode('utf-8'))
        ### initiating consumer
        self.consumer = KafkaConsumer(
                            bootstrap_servers=self.server,
                            auto_offset_reset='earliest',
                            consumer_timeout_ms=100,
                            client_id="sample-client-3",
                            group_id="sample-group2")
        self.consumer.poll(timeout_ms=1)
        self.consumer.subscribe([self.topic])
        ### admin initialization
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.server)
        self.add_topic()
        
    ### creating a topic   
    def add_topic(self):
        try:
            topic_list = [NewTopic(name=self.topic, num_partitions=1, replication_factor=1)]
            self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except:
            pass
        
    def flush_producer(self):
        # Wait for all message to be sent
        self.producer.flush()
    ### message to producer   
    def send_message(self,message): 
        self.producer.send(self.topic,value=message)
        
    # use yield as we could get thousands of messages, and calling function can process these messages without need to wait for all of the messages    
    def consume_messages(self):
        for message in self.consumer:
            self.consumer.commit()
            yield message.value
            
'''
  # reference: https://www.postgresqltutorial.com/postgresql-python/connect/
  1. User registration with created data, address, name and phone with faker
'''

class postgresql_main():
    def __init__(self,service_uri,table_name):
        
        self.service_uri = service_uri
        self.conn = psycopg2.connect(self.service_uri)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        
    def create_tables(self):
        #""" create tables in the PostgreSQL database"""
        command = (f"CREATE TABLE IF NOT EXISTS {self.table_name} (id SERIAL PRIMARY KEY, created_date VARCHAR NOT NULL, User_name VARCHAR NOT NULL,Address VARCHAR NOT NULL, mobile VARCHAR NOT NULL);")
        try:
            self.cursor.execute(command)
        except (Exception, psycopg2.DatabaseError) as error:
              print (error)
    def insert_data(self,message):
        ## insert message data
        sql =  f"INSERT INTO {self.table_name} (created_date,User_name, Address, mobile) VALUES(%s,%s,%s,%s)"
        self.cursor.execute(sql,(message['created_date'], message['User_name'],message['address'],message['phone']))
        #return results
    def get_data(self):
        # retreive data for testing purpose
        self.cursor.execute(f'SELECT * FROM {self.table_name}')
        results = self.cursor.fetchall()
        self.cursor.close()
        return results
    def delete_table(self):
        self.cursor.execute(f'DROP TABLE {self.table_name}')
