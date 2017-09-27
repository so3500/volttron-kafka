import time
import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

'''
example
kafka_topic: "command-from-cloud"
device_point: "fake-campus/fake-building/fake-device/PowerState"
new_value: 1
'''

# produce json messages
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8')
                         )
new_value = 0
while True:
    try:
        menu = int(raw_input('1: Command by user input, 2: Command by json file - '))

        if menu == 1:
            kafka_topic, device_point, new_value = raw_input('input kafka_topic, device_point, new_value: ').split(' ')

            msg = {
                'message': 'message from VOLTTRON to Cloud',
                'new_value': new_value,
                'device_point': device_point
            }
            print('msg: {}\n'.format(msg))
            # send message to broker
            producer.send(kafka_topic, msg)

        elif menu == 2:
            with open('command.json') as f:
                data = json.load(f)
                kafka_topic = data["kafka_topic"]
                device_point_list = data["device_point_list"]

                for device_point_ in device_point_list:
                    device_point = device_point_['device_point']
                    new_value = device_point_["new_value"]
                    msg = {
                        'message': 'message from VOLTTRON to Cloud',
                        'new_value': new_value,
                        'device_point': device_point
                    }
                    print('msg: {}\n'.format(msg))
                    # send message to broker
                    producer.send(kafka_topic, msg)


    except Exception as e:
        print(e)
        break
