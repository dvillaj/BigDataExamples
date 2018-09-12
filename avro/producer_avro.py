import io
import random
import avro.schema
from avro.io import DatumWriter
from kafka import SimpleProducer
from kafka import KafkaClient
from random_words import RandomWords

# To send messages synchronously
KAFKA = KafkaClient('localhost:9092')
PRODUCER = SimpleProducer(KAFKA)

# Kafka topic
TOPIC = "avro-topic"

NUM_MESSAGES = 1

# Path to user.avsc avro schema
SCHEMA_PATH = "user.avsc"
SCHEMA = avro.schema.parse(open(SCHEMA_PATH).read())

colors = ["blue", "red", "black", "white", "pink", "orange", "magenta"]
rw = RandomWords()

for i in xrange(NUM_MESSAGES):
    writer = DatumWriter(SCHEMA)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    
    user_info = {"name": rw.random_words(count=1)[0], 
        "favorite_color": colors[random.randint(0, len(colors)-1)],
        "favorite_number": random.randint(0, 10)}

    writer.write(user_info, encoder)
    raw_bytes = bytes_writer.getvalue()

    print ("Sendind %s ..." % " ".join("{:02x}".format(ord(c)) for c in raw_bytes))
    PRODUCER.send_messages(TOPIC, raw_bytes)