# okafka v2025.09.22

from omnis_calls import sendResponse, sendError
from confluent_kafka import Producer, Consumer, TopicPartition, KafkaException
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema, SchemaRegistryError
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

g_box = Box()


def delivery_report(err, msg):
    if err is not None:
        g_box.delivery_error_message = err
    else:
        g_box.delivery_message = msg


def produce_one(param):
    ret_value = {"success": False}
    g_box.clean_producer()

    try:
        if param is None:
             raise Exception("No params provided")

        producer = _create_producer(param)
        topic = param.get("topic")
        key = param.get("key")
        partition = param.get("partition", -1)
        message = param.get("message")
        headers = _get_headers_as_dict(param.get("headers"))

        producer.produce(topic, value = message, key = key, partition = partition, on_delivery = delivery_report, headers = headers)
        pending_messages = producer.flush(g_box.timeout)
        if pending_messages == 0 and g_box.delivery_error_message is None:
            ret_value.update(_extract_delivery_info(g_box.delivery_message))
            ret_value["success"] = True
        else:
            if g_box.delivery_error_message is None:
                ret_value["error_message"] = "The message queue still has pending messages. Please, check in the broker if the actual message was sent."
            else:
                ret_value["error_message"] = str(g_box.delivery_error_message.code()) + ": " + g_box.delivery_error_message.str()

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["error_message"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def connect_producer(param):
    ret_value = {"success": False}

    if g_box.producer is not None:
        g_box.producer = None

    try:
        if param is None:
            raise Exception("No params provided")

        g_box.producer = _create_producer(param)
        ret_value["success"] = True
    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["error_message"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def close_producer(param):
    if g_box.producer is not None:
        g_box.producer = None

    return sendResponse({"success": True})


def produce(param):
    ret_value = {"success": False}
    g_box.clean_producer()

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.producer is None:
            raise Exception("Producer not open")

        topic = param.get("topic")
        key = param.get("key")
        partition = param.get("partition", 0)
        message = param.get("message")
        headers = _get_headers_as_dict(param.get("headers"))

        g_box.producer.produce(topic, value = message, key = key, partition = partition, on_delivery = delivery_report, headers = headers)
        pending_messages = g_box.producer.flush(g_box.timeout)
        if pending_messages == 0 and g_box.delivery_error_message is None:
            ret_value.update(_extract_delivery_info(g_box.delivery_message))
            ret_value["success"] = True
        else:
            if g_box.delivery_error_message is None:
                ret_value["error_message"] = "The message queue still has pending messages. Please, check in the broker if the actual message was sent."
            else:
                ret_value["error_message"] = str(g_box.delivery_error_message.code()) + ": " + g_box.delivery_error_message.str()

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["error_message"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def connect_consumer(param):
    ret_value = {"success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.consumer is not None:
            g_box.consumer = None

        server = param.get("server")
        topic = param.get("topic")
        client_id = param.get("clientId", "omnis_client")
        group_id = param.get("groupId", "omnis_client_group")
        schema_id = param.get("schemaId")
        partition = param.get("partition")

        conf = {
            "bootstrap.servers": server,
            "group.id": group_id,
            "client.id": client_id,
            "key.deserializer": StringDeserializer("utf_8"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false"
        }

        if schema_id is not None:
            schema_registry_client = SchemaRegistryClient({"url": param.get("schemaRegistryUrl")})
            if schema_id > 0:
                schema_obj = schema_registry_client.get_schema(schema_id)
                avro_deserializer = AvroDeserializer(schema_registry_client = schema_registry_client, schema_str = schema_obj.schema_str)
            else:
                avro_deserializer = AvroDeserializer(schema_registry_client = schema_registry_client, schema_str = None)
            conf["value.deserializer"] = avro_deserializer
        else:
            conf["value.deserializer"] = StringDeserializer("utf_8")

        conf.update(_get_extra_config(param.get("config")))
        g_box.consumer = DeserializingConsumer(conf)
        if partition is None:
            g_box.consumer.subscribe([topic])
        else:
            g_box.consumer.assign([TopicPartition(topic, partition)])
        ret_value["success"] = True

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["error_message"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def close_consumer(param):
    if g_box.consumer is not None:
        g_box.consumer.close()
        g_box.consumer = None

    return sendResponse({"success": True})


def consume(param):
    ret_value = {"success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.consumer is None:
            raise Exception("Consumer not opened")

        msg = g_box.consumer.poll(g_box.timeout)
        if msg is not None:
            if msg.error():
                ret_value["error_message"] = msg.error()
            else:
                ret_value["hasMessage"] = True
                if msg.key() is not None:
                    ret_value["key"] = msg.key()
                else:
                    ret_value["key"] = ""
                ret_value["value"] = msg.value()
                ret_value["offset"] = msg.offset()
                ret_value["partition"] = msg.partition()
                ret_value["topic"] = msg.topic()
                timestamp_type, timestamp_value = msg.timestamp()
                if timestamp_type in [TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME]:
                    ret_value["timestamp"] = datetime.fromtimestamp(timestamp_value / 1000).isoformat()
                else:
                    ret_value["timestamp"] = None
                ret_value["success"] = True
                if msg.headers() is not None:
                    ret_headers = {}
                    for key in msg.headers():
                        ret_headers[key[0]] = key[1].decode()

                    ret_value["headers"] = ret_headers
        else:
            ret_value["hasMessage"] = False
            ret_value["success"] = True
    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["error_message"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def commit(param):
    ret_value = {"success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.consumer is None:
            raise Exception("Consumer not opened")

        partition = param.get("partition")
        offset = param.get("offset")
        topic = param.get("topic")

        g_box.consumer.commit(offsets = [TopicPartition(topic, partition, offset + 1)], asynchronous = False)
        ret_value["success"] = True

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["error_message"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def register_schema(param):
    ret_value = {"success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("url")
        schema_str = param.get("schema")
        subject = param.get("subject")

        src = SchemaRegistryClient({'url': url})
        schema = Schema(schema_str, schema_type = "AVRO")
        schema_id = src.register_schema(subject_name = subject, schema = schema)
        ret_value["schemaId"] = schema_id
        ret_value["success"] = True
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def get_schema_by_subject(param):
    ret_value = {"success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("url")
        subject = param.get("subject")

        sr = SchemaRegistryClient({'url': url})
        latest_version = sr.get_latest_version(subject)

        ret_value["schema"] = latest_version.schema.schema_str
        ret_value["type"] = latest_version.schema.schema_type
        ret_value["version"] = latest_version.version
        ret_value["id"] = latest_version.schema_id
        ret_value["success"] = True
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def get_schema_by_id(param):
    ret_value = {"success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("url")
        id = param.get("id")

        sr = SchemaRegistryClient({'url': url})
        schema = sr.get_schema(id)

        ret_value["schema"] = schema.schema_str
        ret_value["type"] = schema.schema_type
        ret_value["success"] = True
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


# Returns a lista of available subjects
def get_subjects(param):
    ret_value = {"success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("url")

        sr = SchemaRegistryClient({'url': url})
        subjects = sr.get_subjects()

        ret_value["subjects"] = subjects
        ret_value["success"] = True
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


def set_timeout(param):
    ret_value = {"success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        g_box.timeout = param.get("timeout", 60)
        ret_value["success"] = True
    except Exception as ex:
        ret_value["error_message"] = str(ex)

    return sendResponse(ret_value)


#
# Local function. They should not be called from Omnis
#

# Creates and returns a producer instance
def _create_producer(param):
    server = param.get("server")
    schema_id = param.get("schemaId")
    schema_registry_url = param.get("schemaRegistryUrl")

    conf = {
        "bootstrap.servers": server,
        "linger.ms": 0
    }

    conf.update(_get_extra_config(param.get("config")))

    if schema_id is None:
        return Producer(conf)

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    schema_obj = schema_registry_client.get_schema(schema_id)
    avro_serializer = AvroSerializer(
        schema_registry_client = schema_registry_client,
        schema_str = schema_obj.schema_str
    )

    conf["key.serializer"] = StringSerializer("utf_8")
    conf["value.serializer"] = avro_serializer
    return SerializingProducer(conf)


# Extracts information from a Kafka Message and saves it to a dict
def _extract_delivery_info(message):
    info = {
        "offset": message.offset(),
        "partition": message.partition()
    }

    timestamp_type, timestamp_value = message.timestamp()
    if timestamp_type in [TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME]:
        info["timestamp"] = datetime.fromtimestamp(timestamp_value / 1000).isoformat()
    return info


# Reads extra config values that may be provided asd puts it on a dict
def _get_extra_config(param_config):
    conf = {}

    if param_config is not None:
        for item in param_config:
            if len(item) >= 2:
                conf[item[0]] = item[1]

    return conf


# Reads a list os list with headers data and convert it to a dict
def _get_headers_as_dict(lst_headers):
    headers = None

    if lst_headers is not None:
        headers = {}
        for item in lst_headers:
            if len(item) >= 2:
                headers[item[0]] = item[1]

    return headers
