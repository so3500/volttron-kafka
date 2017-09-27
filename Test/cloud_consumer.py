from kafka import KafkaConsumer
import json
import ast
import time
# value_deserializer=lambda m: json.loads(m).decode('utf-8')
consumer = KafkaConsumer(bootstrap_servers=['127.0.0.1:9092'])
consumer.subscribe(['command-to-cloud'])

cnt = 0

while True:
    partition = consumer.poll(timeout_ms=1000, max_records=None)
    # obj : type dict
    if len(partition) > 0:
        # print('poll - receive')
        for p in partition:
            for response in partition[p]:
                print('poll: {}'.format(response))
                # print('poll value: {}'.format(response.value))
                # string to dict
                dic_value = ast.literal_eval(response.value)
                print('\npoll value: {}, new_value: {}'.format(dic_value, dic_value['new_value']))
                # print('poll value type: {}'.format(type(dic_value)))
    else:
        print('poll - no receive yet {}'.format(cnt))
    cnt += 1
