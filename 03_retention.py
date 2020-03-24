import hashlib
import time

# -----------------------------------------------------------------------------
# This step extends the simple Kafka implementation with one
# additional feature: retention time.
#   - a partition actually doesn't store messages, but "records".
#   - a record consists of the message, the key, and a timestamp of
#     when it was created.
#   - a topic has a retention period, after which records and their
#     message will be deleted and can no longer be read.
#
# Retention time does not really have an equivalent in relational
# databases. But you could think of it as an expiration date for rows,
# after which they are deleted.
#
# The reason for this is to control the amount of storage required for
# a broker instance:
#   - if the retention time is for example set to 7 days, a broker
#     never needs more storage than for the amount of messages from
#     one week.
#
# This of course has two major caveats:
#   - clients can not read data older than the retention period. if
#     the message are not stored somewhere else before the expiration
#     date, they are lost.
#   - the amount of storage required for a broker still depends on the
#     amount of messages sent during the retention period. to avoid
#     this, it is possible to specify a message retention limit
#     instead (not implemented here). in that case, clients no longer
#     have guarantees how long messages will be available though.


class Record:
    """A partition actually stores records, which consist of the message
    and some metadata.

    """
    def __init__(self, timestamp, key, message):
        self.timestamp = timestamp
        self.key = key
        self.message = message

    def __repr__(self):
        return 'Record<%s, %s, %s>' % (self.timestamp, self.key, self.message)


class Topic:
    """A topic can have a retention time after which records are deleted."""
    def __init__(self, num_partitions, retention_seconds):
        self.num_partitions = num_partitions
        self.retention_seconds = retention_seconds
        self.partitions = {i: {} for i in range(num_partitions)}

    def vacuum(self):
        """Vacuum deletes records from the partitions which expired their
        retention time.

        Important note: Kafka doesn't actually call this operation
        vacuum, and a database vacuum usually does not delete
        records. Nevertheless the general idea is the same: perform
        some maintenance operation on the stored data.

        """
        now = time.time()
        for partition, records in self.partitions.items():
            print(f'Vacuuming partition {partition}')
            for offset, rec in list(records.items()):
                if rec.timestamp + self.retention_seconds < now:
                    print(f'  Deleting {rec}')
                    del records[offset]
            print('Vacuum done')

    def __repr__(self):
        return str(self.partitions)


class Broker:
    """Still a 'server' of the cluster."""
    def __init__(self):
        self.topics = {}

    def vacuum(self):
        """A broker would periodically run the vacuum operation on its topics
        to manage the storage. To simplify the implementation here it
        needs to be called manually though.

        """
        for topic in self.topics.values():
            topic.vacuum()

    def create_topic(self, name, num_partitions=2, retention_seconds=10):
        """When creating a topic, you can now include the retention time."""
        self.topics[name] = Topic(num_partitions, retention_seconds)

    def send_message(self, topic_name, message, key=None):
        """When sending a message, we actually need to create a record for the
        message. This includes the current timestamp, so that we can
        later check when the record expires and can be removed.

        """
        topic = self.topics[topic_name]
        key = (key or message).encode('utf-8')
        partition = int(hashlib.sha1(key).hexdigest(), 16) % topic.num_partitions
        record = Record(time.time(), key, message)
        offset = self.get_last_offset(topic_name, partition) + 1
        topic.partitions[partition][offset] = record

    def read_message(self, topic_name, partition, offset):
        topic = self.topics[topic_name]
        try:
            return topic.partitions[partition][offset].message
        except Exception:
            return None

    def get_first_offset(self, topic_name, partition):
        """This method is new. Since records can now be deleted, we can no
        longer rely on 0 being the first available offset.

        Note: this isn't an efficient implementation, but it shows
        that offsets no longer correspond to the number of stored
        messages.

        """
        topic = self.topics[topic_name]
        offsets = list(topic.partitions[partition]) or [-1]
        return offsets[0]

    def get_last_offset(self, topic_name, partition):
        topic = self.topics[topic_name]
        offsets = list(topic.partitions[partition]) or [-1]
        return offsets[-1]

    def __repr__(self):
        return str(self.topics)


# -----------------------------------------------------------------------------
# The client does not really need to change, but it can be helpful to
# at least check if messages were missed. This can happen if messages
# are not read fast enough, so that they expire before the client
# tries to read them.

class Consumer:
    def __init__(self, broker, topic, partition, offset=0):
        self.broker = broker
        self.topic = topic
        self.partition = partition
        self.offset = offset

    def get_next_message(self):
        oldest_available_offset = self.broker.get_first_offset(self.topic, self.partition)
        if oldest_available_offset > self.offset:
            print(f'Missed some messages for partition {self.partition} :/')
            print(f'Skipping ahead to offset {oldest_available_offset}.')
            self.offset = oldest_available_offset

        newest_available_offset = self.broker.get_last_offset(self.topic, self.partition)
        if self.offset <= newest_available_offset:
            msg = self.broker.read_message(self.topic, self.partition, self.offset)
            self.offset += 1
            return msg

# -----------------------------------------------------------------------------

# Exercise:
#   1. Create a new 'Broker' instance called "office".
#   2. Create a new topic called "tax-return" with a retention time of 5 seconds.
#   3. Send a few messages to the topic.
#   4. Create a 'Consumer' client called "IRS" for the topic.
#   5. Read and print messages from the client.
#   6. Add different calls to 'time.sleep()' and 'office.vacuum()'
#      between steps 3 to 5 to see what happens when the retention
#      time is reached.
#
# Questions:
#   1. What could the client do when it detects missed messages?
#   2. What are realistic scenarios where a client could miss messages?
