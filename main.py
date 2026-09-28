# okafka v2026.09.22

from omnis_calls import sendResponse
from confluent_kafka import Producer, TopicPartition, KafkaException
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME
from confluent_kafka.admin import AdminClient
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from datetime import datetime

# Global constants
DEFAULT_TIMEOUT = 60
DEFAULT_PARTITION = 0


# Global object for storing data
class Box(object):
    def __init__(self):
        self.delivery_error_message = None
        self.delivery_message = None
        self.consumer = None
        self.producer = None
        self.transactional_id = None

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


# Stores the transactional ID to use on the next connect_producer() call
def set_transactional_id(param):
    ret_value = {"Success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        transactional_id = param.get("TransactionalId")
        if not transactional_id:
            raise Exception("TransactionalId not provided")

        g_box.transactional_id = transactional_id
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Creates the producer and stores it on the global box
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


# Flushes pending messages and releases the producer
def close_producer(param):
    ret_value = {"Success": False}

    try:
        if g_box.producer is not None:
            timeout = param.get("Timeout", DEFAULT_TIMEOUT) if param is not None else DEFAULT_TIMEOUT
            pending_messages = g_box.producer.flush(timeout)
            g_box.producer = None
            g_box.transactional_id = None
            if pending_messages > 0:
                ret_value["ErrorMessage"] = "The producer was closed but " + str(pending_messages) + " message(s) were still pending delivery."
                return sendResponse(ret_value)

        ret_value["Success"] = True
    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
        g_box.producer = None
        g_box.transactional_id = None
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)
        g_box.producer = None
        g_box.transactional_id = None

    return sendResponse(ret_value)


# Produces a single message and blocks until its delivery is confirmed
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
        partition = param.get("Partition", DEFAULT_PARTITION)
        message = param.get("Message")
        timeout = param.get("Timeout", DEFAULT_TIMEOUT)
        headers = _get_headers_as_dict(param.get("Headers"))

        g_box.producer.produce(topic, value = message, key = key, partition = partition, on_delivery = delivery_report, headers = headers)
        pending_messages = g_box.producer.flush(timeout)
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


# Produces a batch of messages, flushing once at the end instead of per message (see MCWKafka's KafkaProducer::produce())
def produce_from_list(param):
    ret_value = {"Success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.producer is None:
            raise Exception("Producer not open")

        topic = param.get("Topic")
        messages = param.get("Messages")
        timeout = param.get("Timeout", DEFAULT_TIMEOUT)

        if not messages:
            ret_value["Results"] = []
            ret_value["Success"] = True
            return sendResponse(ret_value)

        if not isinstance(messages, list) or not all(isinstance(item, dict) for item in messages):
            raise Exception("Messages must be a list or rows")

        deliveries = [None] * len(messages)

        def make_on_delivery(index):
            def _on_delivery(err, msg):
                deliveries[index] = (err, msg)
            return _on_delivery

        for i, item in enumerate(messages):
            key = item.get("Key")
            partition = item.get("Partition", DEFAULT_PARTITION)
            message = item.get("Message")
            headers = _get_headers_as_dict(item.get("Headers"))

            g_box.producer.produce(topic, value = message, key = key, partition = partition, on_delivery = make_on_delivery(i), headers = headers)

            # Poll periodically to trigger delivery callbacks, mirroring MCWKafka's KafkaProducer::produce()
            if (i + 1) % 100 == 0:
                g_box.producer.poll(0)

        pending_messages = g_box.producer.flush(timeout)

        results = []
        has_error = pending_messages > 0
        for err, msg in deliveries:
            if err is not None:
                has_error = True
                results.append({"Success": False, "ErrorMessage": str(err.code()) + ": " + err.str()})
            elif msg is not None:
                item_result = {"Success": True}
                item_result.update(_extract_delivery_info(msg))
                results.append(item_result)
            else:
                has_error = True
                results.append({"Success": False, "ErrorMessage": "Message was not delivered (flush timed out)."})

        ret_value["Results"] = results

        if has_error:
            ret_value["ErrorMessage"] = "One or more messages failed to be delivered. Check Results for details."
        else:
            ret_value["Success"] = True

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Begins a transaction; requires a transactional ID set via set_transactional_id()
def begin_transaction(param):
    ret_value = {"Success": False}

    try:
        if g_box.producer is None:
            raise Exception("Producer not open")

        if g_box.transactional_id is None:
            raise Exception("beginTransaction requires a transactional ID. Call set_transactional_id before connect_producer.")

        g_box.producer.begin_transaction()
        ret_value["Success"] = True

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Commits the current transaction, making its staged messages visible to consumers
def commit_transaction(param):
    ret_value = {"Success": False}

    try:
        if g_box.producer is None:
            raise Exception("Producer not open")

        timeout = param.get("Timeout", DEFAULT_TIMEOUT) if param is not None else DEFAULT_TIMEOUT
        g_box.producer.commit_transaction(timeout)
        ret_value["Success"] = True

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Aborts the current transaction, discarding its staged messages
def abort_transaction(param):
    ret_value = {"Success": False}

    try:
        if g_box.producer is None:
            raise Exception("Producer not open")

        timeout = param.get("Timeout", DEFAULT_TIMEOUT) if param is not None else DEFAULT_TIMEOUT
        g_box.producer.abort_transaction(timeout)
        ret_value["Success"] = True

    except KafkaException as ex:
        kafka_error = ex.args[0]
        ret_value["ErrorMessage"] = str(kafka_error.code()) + ": " + kafka_error.str()
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Creates the consumer and subscribes it to a topic (or assigns a specific partition)
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
        key_schema_id = param.get("KeySchemaId")
        partition = param.get("Partition")

        conf = {
            "bootstrap.servers": server,
            "group.id": group_id,
            "client.id": client_id
        }

        schema_registry_client = None
        if schema_id is not None or key_schema_id is not None:
            schema_registry_client = _create_schema_registry_client(param, param.get("SchemaRegistryUrl"))

        if schema_id is not None:
            conf["value.deserializer"] = _create_avro_deserializer(schema_registry_client, schema_id)
        else:
            conf["value.deserializer"] = StringDeserializer("utf_8")

        if key_schema_id is not None:
            conf["key.deserializer"] = _create_avro_deserializer(schema_registry_client, key_schema_id)
        else:
            conf["key.deserializer"] = StringDeserializer("utf_8")

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


# Closes the consumer and releases it
def close_consumer(param):
    if g_box.consumer is not None:
        g_box.consumer.close()
        g_box.consumer = None

    return sendResponse({"Success": True})


# Polls for a single message and returns it, or HasMessage = False on timeout
def consume(param):
    ret_value = {"Success": False}

    try:
        if param is None:
            raise Exception("No params provided")

        if g_box.consumer is None:
            raise Exception("Consumer not opened")

        timeout = param.get("Timeout", DEFAULT_TIMEOUT)
        msg = g_box.consumer.poll(timeout)
        if msg is not None:
            if msg.error():
                ret_value["ErrorMessage"] = str(msg.error().code()) + ": " + msg.error().str()
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


# Commits the offset of a consumed message synchronously
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


# Registers an Avro schema under a subject in the Schema Registry
def register_schema(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("Url")
        schema_str = param.get("Schema")
        subject = param.get("Subject")

        src = _create_schema_registry_client(param, url)
        schema = Schema(schema_str, schema_type = "AVRO")
        schema_id = src.register_schema(subject_name = subject, schema = schema)
        ret_value["SchemaId"] = schema_id
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Returns the latest schema version registered for a subject
def get_schema_by_subject(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("Url")
        subject = param.get("Subject")

        sr = _create_schema_registry_client(param, url)
        latest_version = sr.get_latest_version(subject)

        ret_value["Schema"] = latest_version.schema.schema_str
        ret_value["Type"] = latest_version.schema.schema_type
        ret_value["Version"] = latest_version.version
        ret_value["Id"] = latest_version.schema_id
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


# Returns a schema by its Schema Registry ID
def get_schema_by_id(param):
    ret_value = {"Success": False}

    try:
        if param is None:
             raise Exception("No params provided")

        url = param.get("Url")
        id = param.get("Id")

        sr = _create_schema_registry_client(param, url)
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

        sr = _create_schema_registry_client(param, url)
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
        
        timeout = param.get("Timeout", DEFAULT_TIMEOUT)
        
        topics = []
        admin = AdminClient(conf)
        cluster_metadata = admin.list_topics(timeout = timeout)
        
        if cluster_metadata is not None:
            for topic in cluster_metadata.topics.values():
                topics.append(topic.topic)

        ret_value["Topics"] = topics
        ret_value["Success"] = True
    except Exception as ex:
        ret_value["ErrorMessage"] = str(ex)

    return sendResponse(ret_value)


#
# Local function. They should not be called from Omnis
#

# Creates a SchemaRegistryClient, applying HTTP Basic-Auth credentials when provided (mirrors MCWKafka's SchemaRegistryClient::setCredentials())
def _create_schema_registry_client(param, url):
    conf = {"url": url}

    user = param.get("SchemaRegistryUser")
    if user:
        password = param.get("SchemaRegistryPassword", "")
        conf["basic.auth.user.info"] = user + ":" + password

    return SchemaRegistryClient(conf)


# Creates and returns a producer instance
def _create_producer(param):
    server = param.get("Server")
    schema_id = param.get("SchemaId", 0)
    schema_str = param.get("Schema", "")
    key_schema_id = param.get("KeySchemaId", 0)
    key_schema_str = param.get("KeySchema", "")
    schema_registry_url = param.get("SchemaRegistryUrl")
    timeout = param.get("Timeout", DEFAULT_TIMEOUT)

    conf = {
        "bootstrap.servers": server
    }

    if g_box.transactional_id is not None:
        conf["transactional.id"] = g_box.transactional_id

    conf.update(_get_extra_config(param.get("Config")))

    has_value_schema = schema_id > 0 or schema_str != ""
    has_key_schema = key_schema_id > 0 or key_schema_str != ""

    if not has_value_schema and not has_key_schema:
        producer = Producer(conf)
    else:
        schema_registry_client = _create_schema_registry_client(param, schema_registry_url)

        if has_value_schema:
            conf["value.serializer"] = _create_avro_serializer(schema_registry_client, schema_id, schema_str)
        else:
            conf["value.serializer"] = StringSerializer("utf_8")

        if has_key_schema:
            conf["key.serializer"] = _create_avro_serializer(schema_registry_client, key_schema_id, key_schema_str)
        else:
            conf["key.serializer"] = StringSerializer("utf_8")

        producer = SerializingProducer(conf)

    if g_box.transactional_id is not None:
        producer.init_transactions(timeout)

    return producer


# Creates an AvroSerializer, resolving the schema from Schema Registry when only an ID is given
def _create_avro_serializer(schema_registry_client, schema_id, schema_str):
    if schema_id > 0:
        schema_obj = schema_registry_client.get_schema(schema_id)
        return AvroSerializer(schema_registry_client = schema_registry_client, schema_str = schema_obj.schema_str)

    return AvroSerializer(schema_registry_client = schema_registry_client, schema_str = schema_str)


# Creates an AvroDeserializer; schema_id <= 0 resolves the writer schema from the message's wire format
def _create_avro_deserializer(schema_registry_client, schema_id):
    if schema_id > 0:
        schema_obj = schema_registry_client.get_schema(schema_id)
        return AvroDeserializer(schema_registry_client = schema_registry_client, schema_str = schema_obj.schema_str)

    return AvroDeserializer(schema_registry_client = schema_registry_client, schema_str = None)


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
