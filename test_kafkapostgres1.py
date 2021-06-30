#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 20:57:20 2021

@author: sowjanyagunturu
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 16:57:34 2021
@author: sowjanyagunturu
"""

import datetime as dt
from faker import Faker
import json
import argparse
fake = Faker()
from kafkaposgres import kafka_cluster,postgresql_main
'''
 This file is used for the argument parser basis on terminal
'''
# I used try and final here to close the resources, but I would suggest to use context manager to realse resources.
def test_kafka(number,kafkaserver,dbserver,password):
    config = json.load(open("config.json"))
    client, db_client, db_read_client = None, None, None
    try:
        client = kafka_cluster(config["kafka"]["server"].replace('kafkahost',kafkaserver),config["kafka"]["topic"])
        db_client = postgresql_main(config["postgres"]["service_uri"].replace('aivenpassword',password).replace('pghost',dbserver),config["postgres"]["table_name"])
        db_read_client = postgresql_main(config["postgres"]["read_uri"].replace('aivenpassword',password).replace('pghost',dbserver),config["postgres"]["table_name"])
        db_client.create_tables()
        for i in range(number):
              message = {
                        'created_date': str(dt.date.today() + dt.timedelta(i)),
                        'User_name': fake.name(),
                        'address':fake.address(),
                        'phone':fake.phone_number()
                    }

              client.send_message(message)
        client.flush_producer()
        print("Sent messages to Kafka cluster")
        messages = client.consume_messages()
        for raw_msg in messages:
            message = json.loads(raw_msg)
            db_client.insert_data(message)
        print ('Inserted data successfully')
        print(db_read_client.get_data())
        ## for drop table
        #db_client.delete_table()
    finally:
        if client:
            client.producer.close()
            client.consumer.close()
        if db_client:           
            db_client.conn.close()
        if db_read_client:
            db_read_client.conn.close();
    
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #parser.add_argument('--path', help="path of configuration file",
     #             required=True)
    parser.add_argument('--kafkahost', help="enter kafka host",
                  required=True)
    parser.add_argument('--dbserver', help="enter postgresql server",
                  required=True)
    parser.add_argument('--dbpassword', help="enter postgresqldb password",
                  required=True)
    args = parser.parse_args()
    test_kafka(4,args.kafkahost,args.dbserver,args.dbpassword)
