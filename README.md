# support-20210711-sowjanya-02
## Kafka-Cluster
Kafka simplifies communication between systems by acting as a centralized communication hub (often likened to a central nervous system), in which systems can send and receive data without knowledge of each other. The communication pattern it implements is called the publish-subscribe pattern (or simply, pub/sub), and the result is a drastically simpler communication model.

1) Instead of having multiple systems communicate directly with each other, producers simply publish their data to one or more topics, without caring who comes along to read the data.

2) Topics are named streams (or channels) of related data that are stored in a Kafka cluster. They serve a similar purpose as tables in a database (i.e., to group related data). However, they do not impose a particular schema, but rather store the raw bytes of data, which makes them very flexible to work with.

3) Consumers are processes that read (or subscribe) to data in one or more topics. They do not communicate directly with the producers, but rather listen to data on any stream they happen to be interested in.

4) Consumers can work together as a group (called a consumer group) in order to distribute work across multiple processes.

Reference: Mastering Kafkastreams and KsqlDB: Mitch Seymour
## Installations Mac osx

 1. Download the latest Apache Kafka from [https://kafka.apache.org/downloads] under Binary downloads.
 
            ---- Test for the java support
            ----cd kafka_folder
            ----sh bin/zookeeper-server-start.sh config/zookeeper.properties
            ----sh bin/kafaka-server-start.sh config/server.properties
            
 2. Used Aiven Postgres sevice for storing the consumed data
 
## Code-Set up

1) Kafkapostgres function defines

       a) To set up the kafkaadminclient in creating a topic

       b) To initialise kafka producer and consumer

       c) Send message to the producer and flush the messages

       d) Read the consumed messages
2) Postgres_main function

       a) Initiating the db server

       b) create table if not exists

       c) Instert consumed data to the db
3) Test Function

       a) create Faker data for the user name,address, mobile

       b) send the message to topic and store the consumed messages in a postgres db

### Commands
     
      1) pip install -r requirements.txt

      2) python test_kafakapostgres.py --path "config.json" and before this uncomment the lines with function args
      
      3) run test_kafakapostgres.py by making changes in the config file with db hostname and password
      
      4) another script test_kafakapostgres1.py --kafkahost name --dbserver name --dbpassword
      

### configuration

      1) kafaka server with service uri

      2) db server with service_uri for creating table and for read purpose use read uri
