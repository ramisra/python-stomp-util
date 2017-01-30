import stomp
import logging


log = logging.getLogger('connect.py')


class StompConnection(stomp.Connection):

        def __init__(self, username, password, host_and_port, wait=True):
            self.username = username
            self.password = password
            self.wait = wait
            self.stomp_connection = stomp.Connection(host_and_port)

        def register_listener(self, listener, listener_alias):

            self.stomp_connection.set_listener(listener_alias, listener())

        def register_listener_dict(self, listener_dict):

            for listener_alias, listener in listener_dict.iteritems():
                self.stomp_connection.set_listener(listener_alias, listener())

        def start_connection(self):
            self.stomp_connection.start()
            self.stomp_connection.connect(self.username, self.password, self.wait)

        def subscribe_to_topic(self, topic):
            self.stomp_connection.subscribe(destination=topic, id='1', ack='auto')

        def subscribe_to_topic_list(self, topic_list):
            for topic in topic_list:
                self.stomp_connection.subscribe(destination=topic, id='1', ack='auto')

        def publish_to_topic(self, topic, message):
            self.stomp_connection.send(topic, message)

        def publish_to_topic_list(self, topic_list, message):
            for topic in topic_list:
                self.stomp_connection.send(topic, message)

        def disconnect(self):
            self.stomp_connection.disconnect()