import sys
import zmq
import time

port = '5556'
if len(sys.argv) > 1:
    port = sys.argv[1]
    int(port)

if len(sys.argv) > 2:
    port1 = sys.argv[2]
    int(port1)

# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print ('Collecting updates from weather server...')
socket.connect('tcp://localhost:%s' % port)

print('connect success port')

if len(sys.argv) > 2:
    socket.connect('tcp://localhost:%s' % port1)

    print('connect success port2')
# Subscribe to zipcode, default is NYC, 10001
topicfilter = '10001'
socket.setsockopt(zmq.SUBSCRIBE, topicfilter)

# Process 5 updates
total_value = 0
# for update_nbr in range(5):
# while True:
while True:
    try:
        # check for a message, this will not block
        string = socket.recv(flags=zmq.NOBLOCK)
        topic, message_data, new_value = string.split()
        # total_value += int(message_data)
        print('string: {}'.format(string))
        print('topic: {}, msg_data: {}, new_value: {}'.format(topic, message_data, new_value))
    except zmq.Again as e:
        print('No message received yet')
        time.sleep(1)

print('Average message_data value for "%s" was %dF' % (topicfilter, total_value / update_nbr))
