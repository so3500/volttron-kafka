import datetime
import logging
import os
import sys
import datetime
from dateutil.parser import parse

# cloud
from pymongo import MongoClient
import zmq
# import asyncio
# import gevent.socket

from volttron.platform.vip.agent import Agent, Core, PubSub
from volttron.platform.messaging import topics
from volttron.platform.agent import utils

utils.setup_logging()
_log = logging.getLogger(__name__)
__version__ = "0.1"

# refre agent creation walkthrough
# link : http://volttron.readthedocs.io/en/4.0.1/devguides/agent_development/Agent-Development.html
# refer example agent
# link : http://volttron.readthedocs.io/en/4.0.1/devguides/agent_development/Agent-Configuration-Store.html#example-agent
def cloud_agent(config_path, **kwargs):
    # get config information
    config = utils.load_config(config_path)
    source = config.get('source')
    destination_ip = config.get('destination_ip')
    destination_port = config.get('destination_port')
    services_topic_list = config.get('services_topic_list')
    database_name = config.get('database_name')
    collection_name = config.get('collection_name')

    if 'all' in services_topic_list:
        services_topic_list = [topics.DRIVER_TOPIC_BASE, topics.LOGGER_BASE,
                            topics.ACTUATOR, topics.ANALYSIS_TOPIC_BASE]

    return CloudAgent(source,
                      destination_ip,
                      destination_port,
                      services_topic_list,
                      database_name,
                      collection_name,
                      **kwargs)

class CloudAgent(Agent):
    def __init__(self, source,
                 destination_ip,
                 destination_port,
                 services_topic_list,
                 database_name,
                 collection_name,
                 **kwargs):
        super(CloudAgent, self).__init__(**kwargs)

        # set config info
        self.source = source
        self.destination_ip = destination_ip
        self.destination_port = destination_port
        self.services_topic_list = services_topic_list
        self.database_name = database_name
        self.collection_name = collection_name

        self.default_config = {"source": source,
                               "destination_ip": destination_ip,
                               "destination_port": destination_port,
                               "services_topic_list": services_topic_list,
                               "database_name": database_name,
                               "collection_name": collection_name}

        self.vip.config.set_default("config", self.default_config)
        # ===========================

        # connect with local(or remote) mongodb
        self.connection = MongoClient(self.destination_ip, int(self.destination_port))
        self.db = self.connection[str(self.database_name)]
        self.collection = self.db[str(self.collection_name)]

        # setting up callback_method for configuration store interface
        # link : http://volttron.readthedocs.io/en/4.0.1/devguides/agent_development/Agent-Configuration-Store.html
        self.vip.config.subscribe(self.configure_new, actions="NEW", pattern="cloud/*")
        self.vip.config.subscribe(self.configure_update, actions=["UPDATE",], pattern="cloud/*")
        self.vip.config.subscribe(self.configure_delete, actions=["DELETE",], pattern="cloud/*")

        # testing command
        self.new_value = 0
        self.write_topic = "fake-campus/fake-building/fake-device/PowerState"
        self.cmd_port = '5556'

        self.conext = zmq.Context()
        self.cmd_subscriber = self.conext.socket(zmq.SUB)
        self.cmd_subscriber.connect("tcp://localhost:{}".format(self.cmd_port))

        # Subscribe to zipcode, default is NYC, 10001
        self.topic_filter = '10001'
        self.cmd_subscriber.setsockopt(zmq.SUBSCRIBE, self.topic_filter)

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

    def post_data(self, peer, sender, bus, topic, headers, message):
        # make post and query to remote(or local) mongod
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

    @Core.receiver("onstart")
    def on_message_topic(self, sender, **kwargs):
        """
        Subscribes to the platform message bus on the actuator, record,
        datalogger, and device topics.
        """
        _log.debug("sender {}, Kwargs {}".format(sender, kwargs))

        def subscriber(subscription, callback_method):
            _log.debug("subscribing to topic : {}".format(subscription))
            self.vip.pubsub.subscribe(peer='pubsub',
                                      prefix=subscription,
                                      callback=callback_method)

        for topic_subscriptions in self.services_topic_list:
            subscriber(topic_subscriptions, self.post_data)

    @Core.periodic(2)
    def actuate_something(self):
        check = False
        try:
            # check for a message, this will not block
            string = self.cmd_subscriber.recv(flags=zmq.NOBLOCK)
            # new_value = string['new_value']
            # write_topic = string['write_topic']
            _log.info("recv from zmqServer: {}.".format(string))
            topic_filter, write_topic, new_value = string.split()
            print('something subscribe : recv string: {0},'.format(string))
            print('topic_filter: {}, write_topic: {}, new_value: {}'.format(topic_filter, write_topic, new_value))
            # print('something subscribe : topic: {0}, message_data: {1}'.format(topic, message_data))
            check = True

        except zmq.Again as e:
            print('No message received yet')
            check = False

        if check is True:
            try:
                result = self.vip.rpc.call(
                    'platform.actuator',
                    'get_point',
                    write_topic,
                ).get(timeout=10)
                _log.info("Reading Before: {}".format(result))

                result = self.vip.rpc.call(
                    'platform.actuator',
                    'set_point',
                    self.core.identity,
                    write_topic,
                    new_value,
                ).get(timeout=10)
                _log.info("Reading After : {}".format(result))

            except Exception as e:
                print(e)

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
