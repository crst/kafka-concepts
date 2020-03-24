# -----------------------------------------------------------------------------
# Very simplified implementation of Kafka concepts:
#   - a Kafka cluster consists of at least one "broker".
#   - each broker can store multiple "topics".
#   - a topic stores a list of arbitrary "messages".
#
# Approximately mapping these terms to a relational database:
#   - a broker corresponds to a database server.
#   - a topic corresponds to a table.
#   - a message corresponds to the values of a row.
#
# Once you have a broker with a topic, there are mainly two operations:
#   1. You can send messages to the broker, which are appended to the topic.
#   2. You can read a message from a specific position of the topic.


class Broker:
    """A 'server' of the cluster."""
    def __init__(self):
        """Each broker can store multiple topics."""
        self.topics = {}

    def create_topic(self, topic_name):
        """Each topic is essentially a list of arbitrary messages."""
        self.topics[topic_name] = []

    def send_message(self, topic_name, message):
        """Messages sent to the broker are appended to the topic."""
        self.topics[topic_name].append(message)

    def read_message(self, topic_name, offset):
        """To read a message, you have to specify the position (offset) of the
        message within the topic list. Since this offset may not
        exist, the broker can return an empty message."""
        try:
            return self.topics[topic_name][offset]
        except Exception:
            return None

    def get_last_offset(self, topic_name):
        """To check if a server has new messages, you can ask for the last
        offset of a topic."""
        return len(self.topics[topic_name])

    def __repr__(self):
        return str(self.topics)


# -----------------------------------------------------------------------------
# To read data from a topic, you usually want to create a consumer
# client. This is necessary, because the broker does not remember
# which offsets you have already read.

class Consumer:
    """A 'client' for reading data from Kafka."""
    def __init__(self, broker, topic_name, offset=0):
        """While not strictly required, each client often reads only one
        specific topic from a specific broker."""
        self.broker = broker
        self.topic_name = topic_name
        # Important: the consumer needs to remember which offsets it
        # has already read!
        self.offset = offset

    def get_next_message(self):
        """Check for messages this client hasn't read yet."""
        newest_available_offset = self.broker.get_last_offset(self.topic_name)
        if self.offset <= newest_available_offset:
            msg = self.broker.read_message(self.topic_name, self.offset)
            self.offset += 1
            return msg


# -----------------------------------------------------------------------------
# Exercise:
#   1. Create a new 'Broker' instance called "office".
#   2. Create two new topics called "Michael" and "Dwight" on the server.
#   3. Send a few different messages to these topics.
#   4. Create two new 'Consumer' client instances, one for each topic.
#   5. Read and print messages from the clients.
