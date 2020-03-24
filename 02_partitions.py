import hashlib

# -----------------------------------------------------------------------------
# This step extends the simple Kafka implementation with one
# additional feature: partitions.
#   - a topic actually consists of one or more "partitions".
#   - a partition stores a list of arbitrary messages.
#   - messages can be send together with a "key". Messages with the
#     same key will always be stored in the same partition.
#
# Approximately mapping these terms to a relational database:
#   - a topic still corresponds to a table.
#   - a partition corresponds to a partition with a subset of data from
#   - the table.
#   - a key corresponds to the value of the distribution key column.
#
# The reason for this is to allow horizontal scaling:
#   - a topic can now be split into different partitions on different
#     servers (brokers).
#   - multiple clients can read messages from a topic without
#     interfering with each other (by only reading from a specific
#     partition).


class Topic:
    """A topic actually consists of a number of partitions. Each partition
    is still a list.

    """
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
        self.partitions = {i: [] for i in range(num_partitions)}

    def __repr__(self):
        return str(self.partitions)


class Broker:
    """A broker is still a 'server' of the cluster."""
    def __init__(self):
        self.topics = {}

    def create_topic(self, topic_name, num_partitions=1):
        """When creating a topic, you can now specify the number of partitions
        the topic is split into.

        Important note: the implementation here is simplified and
        assumes every broker has the 'complete' topic with all
        partitions. In reality this may not be true, and to read/write
        from/to a specific partition you may need to connect to a
        specific broker. This detail can be hidden by adding a
        'Cluster' abstraction which manages the available brokers.

        """
        self.topics[topic_name] = Topic(num_partitions)

    def send_message(self, topic_name, message, key=None):
        """When sending a message, you can now optionally specify a key. This
        key is used to determine in which partition the message is
        stored.

        The actual value of the key is not really too relevant, but it
        guarantees you that messages with the same key will be stored
        in the same partition. This is helpful for two reasons:

          1. messages that belong together can be read by a client
             which only reads from a specific partition.

          2. messages within a partition a guaranteed to be stored in
             the order they were sent. which may not be true for
             messages across different partitions.

        """
        topic = self.topics[topic_name]
        key = (key or message).encode('utf-8')
        partition = int(hashlib.sha1(key).hexdigest(), 16) % topic.num_partitions
        topic.partitions[partition].append(message)

    def read_message(self, topic_name, partition, offset):
        topic = self.topics[topic_name]
        try:
            return topic.partitions[partition][offset]
        except Exception:
            return None

    def get_last_offset(self, topic_name, partition):
        """Getting the last offset now depends on the partition."""
        topic = self.topics[topic_name]
        return len(topic.partitions[partition])

    def __repr__(self):
        return str(self.topics)


# -----------------------------------------------------------------------------
# The client to read data can now be restricted to a specific
# partition.

class Consumer:
    def __init__(self, broker, topic, partition, offset=0):
        self.broker = broker
        self.topic = topic
        self.partition = partition
        self.offset = offset

    def get_next_message(self):
        newest_available_offset = self.broker.get_last_offset(self.topic, self.partition)
        if self.offset <= newest_available_offset:
            msg = self.broker.read_message(self.topic, self.partition, self.offset)
            self.offset += 1
            return msg


# -----------------------------------------------------------------------------
# Exercise:
#   1. Create a new 'Broker' instance called "office".
#   2. Create a new topic called "sales" with 2 partitions on the server.
#   3. Send a few sales numbers to the topic with the keys "Jim",
#      "Dwight", "Michael" and "Todd".
#   4. Create two different 'Consumer' clients "client_0" and
#      "client_1" for the different partitions.
#   5. Read and print messages from the clients.
#
# Questions:
#   1. How could you create only one client instance which reads all
#      sales numbers for Michael?
