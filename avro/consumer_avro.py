import io
import avro.schema
import avro.io
from kafka import KafkaConsumer, TopicPartition

# To consume messages
TOPIC = 'avro-topic'
CONSUMER = KafkaConsumer(TOPIC,
                         bootstrap_servers=['localhost:9092'])

SCHEMA_PATH = "user.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())

for msg in CONSUMER:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(SCHEMA)
    user1 = reader.read(decoder)
    print user1