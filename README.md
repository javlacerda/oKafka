# oKafka
Example of Omnis and Apache Kafka Integration

## Install Apache Kafka using Docker
Type:

    docker pull apache/kafka:latest
    docker run -d -p 9092:9092 --name kafkabroker apache/kafka:latest

After creating the image and the container, type the following for start and stop:

    docker [stop | start] kafkabroker

After installing and running the container, you can try some CLI commands:

    docker exec --workdir /opt/kafka/bin/ -it kafkabroker sh

    ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic
    ./kafka-topics.sh --bootstrap-server localhost:9092 --list
    ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic
    ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning

## Requirements for using Apache Kafka from Omnis
- Install Python 3.x.y
- Install flask, psutils and requirements python packages ("Omnis Application Directory"\pyworker) : `pip install –r requirements`
- Install Python Client API packages from Confluent: `pip install confluent-kafka`
- Copy main.py file to "Omnis Application Directory"\pyworker\okafka

After this, you may use the libraria KAFKADEMO.LBS

## Requirements for using Apache Kafka and Schema Registry from Omnis

Go to the folder containing the file docker-compose.yaml and type:

    docker compose up -d

For stoping the container type:

    docker compose down
