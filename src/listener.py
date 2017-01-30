import stomp
import logging
import time

log = logging.getLogger('listener.py')


class CustomConnectionListener(stomp.ConnectionListener):
    """
    This class extends the stomp Connection Listener class and hence it overrides the some of the
    methods of the same as per the minimal usage . This is default listener class which will inject for the connection
    with stomp if no other listener is passed.
    """
    def on_connecting(self, host_and_port):
        """
        Called by the STOMP connection once a TCP/IP connection to the
        STOMP server has been established or re-established.
        """

        now = int(time.time())
        log.debug("STOMP connection initiated at  message server at %s  ", str(now) + " at host and port "
                  + str(host_and_port))
        print("STOMP connection initiated at  message server at %s  ", str(now) + " at host and port "
              + str(host_and_port))


    def on_connected(self, headers, body):
        """
        Called by the STOMP connection when a CONNECTED frame is
        received
        """

        now = int(time.time())
        log.debug("STOMP connected frame received at  client  %s  ", str(now) + " with headers " + str(headers) +
                  "and body" + str(body))
        print("STOMP connected frame received at  client  %s  ", str(now) + " with headers " + str(headers)
              + "and body" + str(body))


    def on_disconnected(self):
        """
        Called by the STOMP connection when a TCP/IP connection to the
        STOMP server has been lost.  No messages should be sent via
        the connection until it has been reestablished.
        """
        pass

    def on_heartbeat_timeout(self):
        """
        Called by the STOMP connection when a heartbeat message has not been
        received beyond the specified period.
        """
        pass

    def on_before_message(self, headers, body):
        """
        Called by the STOMP connection before a message is returned to the client app. Returns a tuple
        containing the headers and body (so that implementing listeners can pre-process the content).

        :param dict headers: the message headers
        :param body: the message body
        """
        log.debug(" Before getting a message header received  : " + str(headers) + " body for the message " + str(body))
        print(" Before getting a message header received  : " + str(headers) + " body for the message " + str(body))


    def on_message(self, headers, body):
        """
        Called by the STOMP connection when a MESSAGE frame is received.

        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param body: the frame's payload - the message body.
        """
        log.debug("Getting a message header received  : " + str(headers) + " body for the message " + str(body))
        print("Getting a message header received  : " + str(headers) + " body for the message " + str(body))

    def on_receipt(self, headers, body):
        """
        Called by the STOMP connection when a RECEIPT frame is
        received, sent by the server if requested by the client using
        the 'receipt' header.

        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param body: the frame's payload. This is usually empty for RECEIPT frames.
        """
        pass

    def on_error(self, headers, body):
        """
        Called by the STOMP connection when an ERROR frame is received.

        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param body: the frame's payload - usually a detailed error description.
        """
        log.debug("Error occurred getting a message header received  : " + str(headers) + " body for the message "
                  + str(body))
        print("Error occurred while getting  a message header received  : " + str(headers) + " body for the message "
              + str(body))


    def on_send(self, frame):
        """
        Called by the STOMP connection when it is in the process of sending a message

        :param Frame frame: the frame to be sent
        """
        log.debug("Sending a frame : " + str(frame))
        print("Sending a frame : " + str(frame))

    def on_heartbeat(self):
        """
        Called on receipt of a heartbeat.
        """
        pass