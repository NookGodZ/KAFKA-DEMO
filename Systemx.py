import json
import datetime
import random
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "Systemx"
ORDER_LIMIT = 10

producer = KafkaProducer(bootstrap_servers="localhost:29092")

work = ['working','no working']
print('ready to listen')
for i in range(1,ORDER_LIMIT):
    optional = int(input('Select Option Press 0(working) or Press 1(no working) :'))
    time_raw = datetime.datetime.now()
    time_clean = str(time_raw)
    stutus_work = {
        "work_cycle": i,
        "status_work": work[optional],
        "time":time_clean,
    }
    producer.send(ORDER_KAFKA_TOPIC,json.dumps(stutus_work).encode("utf-8"))
    print(stutus_work)
    print(f"Done Sending..{i}")
