from src.connect import StompConnection

from src.listener import CustomConnectionListener


broker_connection = StompConnection('admin', 'password', [('127.0.0.1', 61613)], True)


broker_connection.register_listener(CustomConnectionListener, 'custom')

broker_connection.start_connection()

broker_connection.subscribe_to_topic('topic.test')

broker_connection.publish_to_topic('topic.test','Wrapper testing')
