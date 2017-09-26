from kafka import KafkaConsumer
import json
import ast
import time
# value_deserializer=lambda m: json.loads(m).decode('utf-8')
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                        #  group_id='my-group-2',
                         )
consumer.subscribe(['command-from-cloud'])

while True:
    partition = consumer.poll(timeout_ms=1000, max_records=None)
    # obj : type dict
    if len(partition) > 0:
        print('poll - receive')
        for p in partition:
            for response in partition[p]:
                print('poll: {}'.format(response))
                # print('poll value: {}'.format(response.value))
                # string to dict
                dic_value = ast.literal_eval(response.value)
                print('poll value: {}, new_value: {}'.format(dic_value, dic_value['new_value']))
                print('poll value type: {}'.format(type(dic_value)))
    else:
        print('poll - no receive yet')

# # if differ group name, consume ok
# for message in consumer:
# #message value and key are raw bytes -- decode if necessary
# #e.g., for unicode: `message.value.decode('utf-8')`
#     # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#     #                                      message.offset, message.key,
#     #                                      message.value))
#     # string to dict
#     message_value = ast.literal_eval(message.value)
#     # dict to json
#     # message_value = json.dumps(message_value, indent=2)
#     print('''topic: {}, partition: {},
#            offset: {}, key: {},
#            value: {}\n'''
#            .format(message.topic, message.partition, message.offset, message.key, message.value))
#     print('message.value.message dict: {}\n'.format((message.value)))
#     # print('message.value class: {}\n'.format(message_value.title))
#     # print('message.value.meesage json: {}'.format(json.dumps(message)))
