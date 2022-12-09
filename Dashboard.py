import json
import time
import random
from kafka import KafkaConsumer

consumer = KafkaConsumer('Systemx', bootstrap_servers='localhost:29092')

work = ['working','no working']
print('ready to listen')
while True:
    for message in consumer:
    
        consumed_message = json.loads(message.value.decode())
        work_cycle_id = consumed_message["work_cycle"]
        status_work = consumed_message["status_work"]
        work_time =  consumed_message["time"]
        if  status_work == 0:
            print('working')
        elif status_work == 1:
            print('no working')

        print("Empolyee 1 :",status_work,", TIME :",work_time)
