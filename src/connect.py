import stomp
import logging
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool

log = logging.getLogger('connect.py')


class Singleton(type):
    def __init__(cls, name, bases, dic):
        super(Singleton, cls).__init__(name, bases, dic)
        cls.instance = None

    def __call__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kwargs)

        return cls.instance


class StompConnection(stomp.Connection):

        __metaclass__ = Singleton

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
            self.stomp_connection.send(destination=topic, body=message)

        def publish_to_topic_list(self, topic_list, message):
            for topic in topic_list:
                self.stomp_connection.send(destination=topic, body=message)

        def push_multiple_message_parallel(self,topic,message,number_of_times):
            number_of_thread_to_spawn = multiprocessing.cpu_count()
            pool = ThreadPool(number_of_thread_to_spawn)
            results = pool.map(self.publish_to_topic, number_of_times ,)

        def disconnect(self):
            self.stomp_connection.disconnect()




