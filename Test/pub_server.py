import zmq
import random
import sys
import time

port = '5556'
if len(sys.argv) > 1:
    port = sys.argv[1]
    int(port)

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind('tcp://*:%s' % port)

new_value = 0
write_topic = "fake-campus/fake-building/fake-device/PowerState"

# message_data = {
#     'new_value': 0,
#     'write_topic': "fake-campus/fake-building/fake-device/temperature"
# }

message_data = '{},{}'.format(new_value, write_topic)

while True:
    topic = random.randrange(9999, 10005)
    new_value = random.randrange(1, 215) - 80
    message_data = '{} {}'.format(write_topic, new_value)
    # message_data['new_value'] = new_value
    print('{} {}'.format(topic, message_data))
    socket.send('{} {}'.format(topic, message_data))
    time.sleep(1)
