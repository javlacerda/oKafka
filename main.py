# okafka v2026.01.19

from omnis_calls import sendResponse
from confluent_kafka import Producer, TopicPartition, KafkaException
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME
from confluent_kafka.admin import AdminClient
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from datetime import datetime

# Global object for storing data
class Box(object):
    def __init__(self):
        self.delivery_error_message = None
        self.delivery_message = None
        self.consumer = None
        self.producer = None
        self.timeout = 60

    def clean_producer(self):
        self.delivery_error_message = None
        self.delivery_message = None


# Global code
g_box = Box()


# Callback function used by producers
def delivery_report(err, msg):
    if err is not None:
        g_box.delivery_error_message = err
    else:
        g_box.delivery_message = msg


# Connects to server and produce one message
def produce_one(param):
    ret_value = {"Success": False}
    g_box.clean_producer()

    try:
        if param is None:
             raise Exception("No params provided")

        producer = _create_producer(param)
        topic = param.get("Topic")
        key = param.get("Key")
        partition = param.get("Partition", -1)
        message = param.get("Message")
        headers = _get_headers_as_dict(param.get("Headers"))

        producer.produce(topic, key = key, value = message, partition = partition, on_delivery = delivery_report, headers = headers)
        pending_messages = producer.flush(g_box.timeout)

        if pending_messages == 0 and g_box.delivery_error_message is None:
            ret_value.update(_extract_delivery_info(g_box.delivery_message))
            ret_value["Success"] = True
        else:
            if g_box.delivery_error_message is None:
                ret_value["ErrorMessage"] = "The message queue still has pending messages. Please, check in the broker if the actual message was sent."
            else:
                ret_value["ErrorMessage"] = str(g_box.delivery_error_message.code()) + ": " + g_box.delivery_error_message.str()

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def connect_producer(param):
    ret_value = {"Success": False}

    if g_box.producer is not None:
        g_box.producer = None

    try:
        if param is None:
            raise Exception("No params provided")

        g_box.producer = _create_producer(param)
        ret_value["Success"] = True
    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def close_producer(param):
    if g_box.producer is not None:
        g_box.producer = None

    return sendResponse({"Success": True})


def produce(param):
    ret_value = {"Success": False}
    g_box.clean_producer()

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.producer is None:
            raise Exception("Producer not open")

        topic = param.get("Topic")
        key = param.get("Key")
        partition = param.get("Partition", 0)
        message = param.get("Message")
        headers = _get_headers_as_dict(param.get("Headers"))

        g_box.producer.produce(topic, value = message, key = key, partition = partition, on_delivery = delivery_report, headers = headers)
        pending_messages = g_box.producer.flush(g_box.timeout)
        if pending_messages == 0 and g_box.delivery_error_message is None:
            ret_value.update(_extract_delivery_info(g_box.delivery_message))
            ret_value["Success"] = True
        else:
            if g_box.delivery_error_message is None:
                ret_value["ErrorMessage"] = "The message queue still has pending messages. Please, check in the broker if the actual message was sent."
            else:
                ret_value["ErrorMessage"] = str(g_box.delivery_error_message.code()) + ": " + g_box.delivery_error_message.str()

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def connect_consumer(param):
    ret_value = {"Success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.consumer is not None:
            g_box.consumer = None

        server = param.get("Server")
        topic = param.get("Topic")
        client_id = param.get("ClientId", "omnis_client")
        group_id = param.get("GroupId", "omnis_client_group")
        schema_id = param.get("SchemaId")
        partition = param.get("Partition")

        conf = {
            "bootstrap.servers": server,
            "group.id": group_id,
            "client.id": client_id,
            "key.deserializer": StringDeserializer("utf_8"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false"
        }

        if schema_id is not None:
            schema_registry_client = SchemaRegistryClient({"url": param.get("SchemaRegistryUrl")})
            if schema_id > 0:
                schema_obj = schema_registry_client.get_schema(schema_id)
                avro_deserializer = AvroDeserializer(schema_registry_client = schema_registry_client, schema_str = schema_obj.schema_str)
            else:
                avro_deserializer = AvroDeserializer(schema_registry_client = schema_registry_client, schema_str = None)
            conf["value.deserializer"] = avro_deserializer
        else:
            conf["value.deserializer"] = StringDeserializer("utf_8")

        conf.update(_get_extra_config(param.get("Config")))
        g_box.consumer = DeserializingConsumer(conf)
        if partition is None:
            g_box.consumer.subscribe([topic])
        else:
            g_box.consumer.assign([TopicPartition(topic, partition)])
        ret_value["Success"] = True

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def close_consumer(param):
    if g_box.consumer is not None:
        g_box.consumer.close()
        g_box.consumer = None

    return sendResponse({"Success": True})


def consume(param):
    ret_value = {"Success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.consumer is None:
            raise Exception("Consumer not opened")

        msg = g_box.consumer.poll(g_box.timeout)
        if msg is not None:
            if msg.error():
                ret_value["ErrorMessage"] = msg.error()
            else:
                ret_value["HasMessage"] = True
                if msg.key() is not None:
                    ret_value["Key"] = msg.key()
                else:
                    ret_value["Key"] = ""
                ret_value["Value"] = msg.value()
                ret_value["Offset"] = msg.offset()
                ret_value["Partition"] = msg.partition()
                ret_value["Topic"] = msg.topic()
                timestamp_type, timestamp_value = msg.timestamp()
                if timestamp_type in [TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME]:
                    ret_value["Timestamp"] = datetime.fromtimestamp(timestamp_value / 1000).isoformat()
                else:
                    ret_value["Timestamp"] = None
                ret_value["Success"] = True
                
                if msg.headers() is not None:
                    ret_headers = []
                    for item in msg.headers():
                        ret_headers.append((item[0], item[1].decode()))

                    ret_value["Headers"] = ret_headers
        else:
            ret_value["HasMessage"] = False
            ret_value["Success"] = True
    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def commit(param):
    ret_value = {"Success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.consumer is None:
            raise Exception("Consumer not opened")

        partition = param.get("Partition")
        offset = param.get("Offset")
        topic = param.get("Topic")

        g_box.consumer.commit(offsets = [TopicPartition(topic, partition, offset + 1)], asynchronous = False)
        ret_value["Success"] = True

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def register_schema(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("Url")
        schema_str = param.get("Schema")
        subject = param.get("Subject")

        src = SchemaRegistryClient({'url': url})
        schema = Schema(schema_str, schema_type = "AVRO")
        schema_id = src.register_schema(subject_name = subject, schema = schema)
        ret_value["SchemaId"] = schema_id
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def get_schema_by_subject(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("Url")
        subject = param.get("Subject")

        sr = SchemaRegistryClient({'url': url})
        latest_version = sr.get_latest_version(subject)

        ret_value["Schema"] = latest_version.schema.schema_str
        ret_value["Type"] = latest_version.schema.schema_type
        ret_value["Version"] = latest_version.version
        ret_value["Id"] = latest_version.schema_id
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def get_schema_by_id(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("Url")
        id = param.get("Id")

        sr = SchemaRegistryClient({'url': url})
        schema = sr.get_schema(id)

        ret_value["Schema"] = schema.schema_str
        ret_value["Type"] = schema.schema_type
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Returns a list of available subjects
def get_subjects(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("Url")

        sr = SchemaRegistryClient({'url': url})
        subjects = sr.get_subjects()

        ret_value["Subjects"] = subjects
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Returns a list of topics
def get_topics(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")
    
        conf = {
            'bootstrap.servers': param.get("Server")
        }
        
        conf.update(_get_extra_config(param.get("Config")))
        
        topics = []
        admin = AdminClient(conf)
        cluster_metadata = admin.list_topics()
        
        if cluster_metadata is not None:
            for topic in cluster_metadata.topics.values():
                topics.append(topic.topic)

        ret_value["Topics"] = topics
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


def set_timeout(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        g_box.timeout = param.get("Timeout", 60)
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


#
# Local function. They should not be called from Omnis
#

# Creates and returns a producer instance
def _create_producer(param):
    server = param.get("Server")
    schema_id = param.get("SchemaId", 0)
    schema_str = param.get("Schema", "")
    schema_registry_url = param.get("SchemaRegistryUrl")

    conf = {
        "bootstrap.servers": server,
        "linger.ms": 0
    }

    conf.update(_get_extra_config(param.get("Config")))

    if schema_id <= 0 and schema_str == "":
        return Producer(conf)

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    if schema_id > 0:
        schema_obj = schema_registry_client.get_schema(schema_id)
        avroSerializer = AvroSerializer(schema_registry_client = schema_registry_client, schema_str = schema_obj.schema_str)
    else:
        avroSerializer = AvroSerializer(schema_registry_client = schema_registry_client, schema_str = schema_str)

    conf["key.serializer"] = StringSerializer("utf_8")
    conf["value.serializer"] = avroSerializer
    return SerializingProducer(conf)


# Extracts information from a Kafka Message and saves it to a dict
def _extract_delivery_info(message):
    info = {
        "Offset": message.offset(),
        "Partition": message.partition()
    }

    timestamp_type, timestamp_value = message.timestamp()
    if timestamp_type in [TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME]:
        info["Timestamp"] = datetime.fromtimestamp(timestamp_value / 1000).isoformat()
    return info


# Reads extra config values that may be provided asd puts it on a dict
def _get_extra_config(param_config):
    conf = {}

    if param_config is not None:
        for item in param_config:
            if len(item) >= 2:
                conf[item[0]] = item[1]

    return conf


# Reads a list of lists with headers data and convert it to a dict
def _get_headers_as_dict(lst_headers):
    headers = None

    if lst_headers is not None:
        headers = {}
        for item in lst_headers:
            if len(item) >= 2:
                headers[item[0]] = item[1]

    return headers
