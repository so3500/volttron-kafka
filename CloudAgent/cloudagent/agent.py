import datetime
import logging
import os
import sys
import datetime
import json
from dateutil.parser import parse
import multiprocessing
import ast
import random

# cloud
from pymongo import MongoClient

# kafka
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from volttron.platform.vip.agent import Agent, Core, PubSub
from volttron.platform.messaging import topics
from volttron.platform.agent import utils

utils.setup_logging()
_log = logging.getLogger(__name__)
__version__ = "0.2"

# refre agent creation walkthrough
# link : http://volttron.readthedocs.io/en/4.0.1/devguides/agent_development/Agent-Development.html
# refer example agent
# link : http://volttron.readthedocs.io/en/4.0.1/devguides/agent_development/Agent-Configuration-Store.html#example-agent
def cloud_agent(config_path, **kwargs):
    '''
        Function: Return CloudAgent object with configuration information

        Args: Same with Class Args

        Returns: CloudAgent object

        Note: None

        Created: JinhoSon, 2017-04-14
        Deleted: .
    '''
    # get config information
    config = utils.load_config(config_path)
    source = config.get('source')
    destination_ip = config.get('destination_ip')
    destination_port = config.get('destination_port')
    services_topic_list = config.get('services_topic_list')
    database_name = config.get('database_name')
    collection_name = config.get('collection_name')
    command_topic = config.get('command_topic')
    cloud_broker_ip = config.get('cloud_broker_ip')
    cloud_broker_port = config.get('cloud_broker_port')
    cloud_producer_topic = config.get('cloud_producer_topic')
    cloud_consumer_topic = config.get('cloud_consumer_topic')

    if 'all' in services_topic_list:
        services_topic_list = [topics.DRIVER_TOPIC_BASE, topics.LOGGER_BASE,
                            topics.ACTUATOR, topics.ANALYSIS_TOPIC_BASE]

    return CloudAgent(source,
                      destination_ip,
                      destination_port,
                      services_topic_list,
                      database_name,
                      collection_name,
                      command_topic,
                      cloud_broker_ip,
                      cloud_broker_port,
                      cloud_producer_topic,
                      cloud_consumer_topic,
                      **kwargs)

class CloudAgent(Agent):
    '''
    ----------------------------------------------------------------------------
    Agent summary
    ----------------------------------------------------------------------------
        Name: CloudAgent

        Version: 0.2

        Function:
            1. Subscribe data from message bus
            2. Send device data to Cloud(MongoDB)
            3. Send command history to Cloud(MongoDB)
            4. Send message(command) to Cloud(Kafka consumer)
            5. Receive message(command) from Cloud(Kafka producer)
            6. Publish data to message bus(test for command)
            7. Command to device point using RPC

        Args:
            source (str): zone name
            destination_ip (str): MongoDB server ip in Cloud
            destination_port (str): MongoDB server port in Cloud
            services_topic_list (list): Topic Data sended to MongoDB server in Cloud
            database_name (str): MongoDB database name(like database)
            collection_name (str): MongoDB collection name(like table)
            command_topic (str): When CloudAgent receives a command from another agent,
                                 the topic that is uesd when other agents publih to MessageBus
            cloud_broker_ip (str): Kafka Broker ip in Cloud
            cloud_broker_port (str): Kafka Broker port in Cloud
            cloud_producer_topic (str): Topic for messaging(commanding) from Cloud to VOLTTRON
            cloud_consumer_topic (str): Topic for messaging(commanding) from VOLTTRON to Cloud

        Returns:
            None

        Note:
            Version 0.1: Add - Function 1, 2
            Version 0.2: Add - Function 3, 4, 5, 6, 7
    '''

    '''
    History
    =====
    Create '__init__' (by JinhoSon, 2017-04-14)
    Create 'post_data' (by SungonLee, 2017-04-14)
    Create 'on_message_topic' (by SungonLee, 2017-04-20)
    Create 'subscriber' (by SungonLee, 2017-04-20)
    Create 'actuate_something' (by SungonLee, 2017-07-20)
    Create 'publish_command' (by SungonLee, 2017-09-10)
    Create 'command_to_cloud' (by SungonLee, 2017-09-10)
    Modify '__init__' (by SungonLee, 2017-09-20)
    Create 'command_to_cloud_' (by SungonLee, 2017-09-20)
    Delete 'command_to_cloud_' (by SungonLee, 2017-09-23)

    '''

    def __init__(self, source,
                 destination_ip,
                 destination_port,
                 services_topic_list,
                 database_name,
                 collection_name,
                 command_topic,
                 cloud_broker_ip,
                 cloud_broker_port,
                 cloud_producer_topic,
                 cloud_consumer_topic,
                 **kwargs):
        '''
            Function:
                1. initiallizing the configuration information
                2. Create Connection with MongoDB server, Kafka Consumer, Kafka Producer in Cloud

            Args: Same with Class Args

            Returns: None

            Note:
                self.connection: connection with MongoDB in Cloud
                self.consumer: connection with kafka consumer in Cloud
                self.producer: connection with kafka producer in Cloud

            Created: JinhoSon, 2017-04-14
            Modified: SungonLee, 2017-09-20
            Deleted: .
        '''
        super(CloudAgent, self).__init__(**kwargs)

        # set config info
        self.source = source
        self.destination_ip = destination_ip
        self.destination_port = destination_port
        self.services_topic_list = services_topic_list
        self.database_name = database_name
        self.collection_name = collection_name
        self.command_topic = command_topic
        self.cloud_broker_ip = cloud_broker_ip
        self.cloud_broker_port = cloud_broker_port
        self.cloud_producer_topic = cloud_producer_topic
        self.cloud_consumer_topic = cloud_consumer_topic

        self.default_config = {"source": source,
                               "destination_ip": destination_ip,
                               "destination_port": destination_port,
                               "services_topic_list": services_topic_list,
                               "database_name": database_name,
                               "collection_name": collection_name,
                               "command_topic": command_topic,
                               "cloud_broker_ip": cloud_broker_ip,
                               "cloud_broker_port": cloud_broker_port,
                               "cloud_producer_topic": cloud_producer_topic,
                               "cloud_consumer_topic": cloud_consumer_topic
                               }

        _log.info('default_config: {}'.format(self.default_config))

        self.vip.config.set_default("config", self.default_config)

        # setting up callback_method for configuration store interface
        self.vip.config.subscribe(self.configure_new, actions="NEW", pattern="cloud/*")
        self.vip.config.subscribe(self.configure_update, actions=["UPDATE",], pattern="cloud/*")
        self.vip.config.subscribe(self.configure_delete, actions=["DELETE",], pattern="cloud/*")

        self.new_value_ = 0

        # connect with local(or remote) mongodb
        self.connection = MongoClient(self.destination_ip, int(self.destination_port))
        self.db = self.connection[str(self.database_name)]
        self.collection = self.db[str(self.collection_name)]


        # kafka
        self.cloud_producer_addr = '{0}:{1}'.format(self.cloud_broker_ip, self.cloud_broker_port)
        self.consumer = KafkaConsumer(bootstrap_servers=[self.cloud_producer_addr])
        self.consumer.subscribe([self.cloud_producer_topic])

        # kafak producer - command volttron to cloud
        # produce json messages
        self.cloud_consumer_addr = '{0}:{1}'.format(self.cloud_broker_ip, self.cloud_broker_port)
        self.producer = KafkaProducer(bootstrap_servers=[self.cloud_consumer_addr],
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )

    # configuration callbacks
    # lnke : http://volttron.readthedocs.io/en/4.0.1/devguides/agent_development/Agent-Configuration-Store.html
    # Ensure that we use default values from anything missing in the configuration
    def configure_new(self, config_name, action, contents):
        _log.debug("configure_new")
        config = self.default_config.copy()
        config.update(contents)

    # update cloud agent config
    def configure_update(self, config_name, action, contents):
        _log.debug("configure_update")

    # delete cloud agent config
    def configure_delete(self, config_name, action, contents):
        _log.debug("configure_delete")

    def post_data(self, peer=None, sender=None, bus=None, topic=None, headers=None, message=None):
        '''
            Function: Send device data to Cloud(MongoDB).

            Args:
                peer: the ZMQ identity of the bus owner sender is identity of the publishing peer
                sender: identity of agent publishing messages to messagebus
                bus:
                topic: the full message topic
                headers: case-insensitive dictionary (mapping) of message headers
                message: possibly empty list of message parts

            Returns: None

            Note:
                callback method for subscribing.
                subscribe message topic: actuator, record, datalogger and device topics send data to MongoDB(Cloud or Local)

            Created: JinhoSon, 2017-04-14
            Modified: SungonLee, 2017-5-20
            Deleted: .
        '''
        try:
            _log.info('Post_data: subscribe from message bus, topic:{0}, message:{1}, sender:{2}'
            .format(topic ,message, sender, bus))

            post = {
                'author': 'volttron.cloudagnet',
                'source': self.source,
                'date': str(datetime.datetime.now()),
                'topic': topic,
                'headers': headers,
                'message': message,
            }
            post_id = self.collection.insert(post)
            _log.debug('mongodb insertion success topic : {}, message : {}'
            .format(topic, message))
        except Exception as e:
            _log.error('Post_data: {}'.format(e))

    def command_to_cloud(self, peer, sender, bus, topic, headers, message):
        '''
            Function:
                Send Command to Cloud.
                Send Command history to Cloud(MongoDB).

            Args: Same with 'post_data'

            Returns: None

            Note:
                Callback method for subscribing.
                Subscribe message topic: 'command-to-cloud' send command to cloud,
                                         producer(CloudAgent)-> kafka broker(Cloud) -> consumer(Cloud)

            Created: SungonLee, 2017-09-10
            Deleted: .
        '''
        try:
            _log.info('Command_to_cloud: subscribe from message bus, topic:{0}, message:{1}, sender:{2}'
                      .format(topic ,message, sender, bus))

            new_value = message[0]
            msg = {'from': 'CloudAgent', 'to':'Cloud'
                  ,'message': 'message from VOLTTRON to Cloud', 'new_value': new_value}

            # Send command to Consumer(in Cloud)
            self.producer.send(self.cloud_consumer_topic, msg)
            # Send command data to MongoDB(in Cloud)
            self.post_data(topic=self.cloud_consumer_topic, message=msg)

        except Exception as e:
            _log.error('Command_to_cloud: {}'.format(e))

    @Core.receiver("onstart")
    def on_message_topic(self, sender, **kwargs):
        '''
            Function: Resister callback method for sending data(device data, command history) to Cloud(MongoDB).

            Args: .

            Returns: None

            Note:
                This method is executed after '__init__' method.
                Subscribes to the platform message bus on the actuator, record, datalogger, and device topics.

            Created: JinhoSon, 2017-04-14
            Modified: SungonLee, 2017-05-20
            Deleted: .
        '''
        _log.debug("sender {}, Kwargs {}".format(sender, kwargs))

        # Define method for resistering callback method
        def subscriber(subscription, callback_method):
            '''
                Args:
                    subscription: topic (e.g. "devices/fake-campus/fake-building/fake-device/PowerState")
                    callback_method: method resistered

                Note:
                    callback_mothod: 'post_data', 'command_to_cloud'
            '''
            _log.debug("Subscribing to topic : {}".format(subscription))
            self.vip.pubsub.subscribe(peer='pubsub',
                                      prefix=subscription,
                                      callback=callback_method)

        # Resister callback method with 'subscriber'
        for topic_subscriptions in self.services_topic_list:
            subscriber(topic_subscriptions, self.post_data)

        subscriber(self.command_topic, self.command_to_cloud)


    @Core.periodic(1)
    def actuate_something(self):
        '''
            Function:
                Receive message(command) from Cloud(Kafka broker).
                Use RPC to set device point value with message infomation.

            Args: None
            Returns: None
            Note: None
            Created: SungonLee, 2017-07-20
            Modified: SungonLee, 2017-09-20
            Deleted: .
        '''
        # partition type : nametuple
        # if timeout_ms is 0, check that is there any message in broker imm
        partition = self.consumer.poll(timeout_ms=0, max_records=None)

        try:
            if len(partition) > 0:
                for p in partition:
                    for response in partition[p]:
                        # convert string to dictionary
                        response_dict = ast.literal_eval(response.value)
                        _log.info('Actuate_something: Receive message from cloud message: {}, new_value: {}'
                        .format(response_dict, response_dict['new_value']))

                        new_value = response_dict['new_value']
                        device_point = response_dict['device_point']

                        # Use RPC to get point-value in device
                        result = self.vip.rpc.call(
                            'platform.actuator',
                            'get_point',
                            device_point
                        ).get(timeout=10)
                        _log.info("Actuate_something: Reading Before commmand: {}".format(result))

                        # Use RPC to set point-value in device
                        result = self.vip.rpc.call(
                            'platform.actuator',
                            'set_point',
                            self.core.identity,
                            device_point,
                            new_value,
                        ).get(timeout=10)
                        _log.info("Actuate_something: Reading After command: {}".format(result))

                        # Send command data to MongoDB(in Cloud)
                        msg = {'from': 'Cloud',
                               'to':'CloudAgent',
                               'message': 'message from Cloud to VOLTTRON',
                               'device_point': device_point,
                               'new_value': new_value}
                        self.post_data(topic=self.cloud_producer_topic, message=msg)

            else:
                _log.info('Actuate_something: Not receive command from cloud')
        except Exception as e:
            _log.error('Actuate_something: {}'.format(e))

    @Core.periodic(5)
    def publish_command(self):
        '''
            Function:
                Publish message(command) to MessageBus(VOLTTRON)
                after that CloudAgent subscribes this message from MessageBus
                after that CloudAgent sends this message(command) to Cloud.

            Args: .

            Returns: None

            Note:
                Test method for publishing example message to MessageBus.
                Publish message(command) to MessageBus(VOLTTRON) with topic in config file 'command_topic'.
                Period for Publishing message can be exchanged(current 5s).

            Created: SungonLee, 2017-09-20
            Deleted: .
        '''
        try:
            # Create time, message, value info
            headers = {
                'date': str(datetime.datetime.now())
            }
            message = [
                self.new_value_,
               {
                   'message': 'message VOLTTRON to Cloud',
                   'new_value': self.new_value_,
               }
            ]
            self.new_value_ += 1
            topic = self.command_topic

            self.vip.pubsub.publish('pubsub', topic, headers, message)

            _log.info('Publish_command: publish to message bus, topic:{0}, new_value_:{1}, message:{2}'
                      .format(topic ,self.new_value_, 'message VOLTTRON to Cloud'))
        except Exception as e:
            _log.error('Publish_command: {}'.format(e))

        # @Core.periodic(5)
        # def command_to_cloud_(self):
        #     '''
        #         Function:
        #             Send message(command) to Cloud(Kafka broker)
        #
        #         Args: .
        #
        #         Returns: None
        #
        #         Note:
        #             Test method for sending example message to Cloud.
        #             Period for sending message can be exchanged(current 5s).
        #
        #         Created: SungonLee, 2017-09-20
        #         Deleted: SungonLee, 2017-09-23
        #     '''
        #     try:
        #         new_value = random.randrange(200, 300)
        #         msg = {'title': 'cloud-title', 'message': 'volttron_to_cloud', 'new_value': new_value}
        #         # j_msg = json.dumps(msg)
        #         # print('mag: {}\nj_msg: {}\n\n'.format(msg, j_msg))
        #         _log.info('Command msg: {}\n'.format(msg))
        #         #  sent('topic', value)
        #         self.producer.send('cloud-topic', msg)
        #
        #     except Exception as e:
        #         _log.error('Command_to_cloud: {}'.format(e))

def main(argv=sys.argv):
    '''Main method called to start the agent.'''
    utils.vip_main(cloud_agent, identity='cloudagent',
                   version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass

