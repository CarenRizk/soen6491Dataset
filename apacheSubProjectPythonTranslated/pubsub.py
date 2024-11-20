from typing import Any
from typing import Optional

from apache_beam.transforms import PTransform
from google.cloud import pubsub

__all__ = [
    'MultipleReadFromPubSub',
    'PubsubMessage',
    'PubSubSourceDescriptor',
    'ReadFromPubSub',
    'ReadStringsFromPubSub',
    'WriteStringsToPubSub',
    'WriteToPubSub'
]


class PubsubMessage(object):
    """Represents a Cloud Pub/Sub message.

    Message payload includes the data and attributes fields. For the payload to be
    valid, at least one of its fields must be non-empty.

    Attributes:
        data: (bytes) Message data. May be None.
        attributes: (dict) Key-value map of str to str, containing both user-defined
            and service generated attributes (such as id_label and
            timestamp_attribute). May be None.
        message_id: (str) ID of the message, assigned by the pubsub service when the
            message is published. Guaranteed to be unique within the topic. Will be
            reset to None if the message is being written to pubsub.
        publish_time: (datetime) Time at which the message was published. Will be
            reset to None if the Message is being written to pubsub.
        ordering_key: (str) If non-empty, identifies related messages for which
            publish order is respected by the PubSub subscription.
    """
    def __init__(
            self,
            data,
            attributes,
            message_id=None,
            publish_time=None,
            ordering_key=""):
        if data is None and not attributes:
            raise ValueError(
                'Either data (%r) or attributes (%r) must be set.', data, attributes)
        self.data = data
        self.attributes = attributes
        self.message_id = message_id
        self.publish_time = publish_time
        self.ordering_key = ordering_key

    def __hash__(self):
        return hash((self.data, frozenset(self.attributes.items())))

    def __eq__(self, other):
        return isinstance(other, PubsubMessage) and (
                self.data == other.data and self.attributes == other.attributes)

    def __repr__(self):
        return 'PubsubMessage(%s, %s)' % (self.data, self.attributes)

    @staticmethod
    def _from_proto_str(proto_msg: bytes) -> 'PubsubMessage':
        """Construct from serialized form of ``PubsubMessage``.

        Args:
            proto_msg: String containing a serialized protobuf of type
            https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#google.pubsub.v1.PubsubMessage

        Returns:
            A new PubsubMessage object.
        """
        msg = pubsub.types.PubsubMessage.deserialize(proto_msg)
        attributes = dict((key, msg.attributes[key]) for key in msg.attributes)
        return PubsubMessage(
            msg.data,
            attributes,
            msg.message_id,
            msg.publish_time,
            msg.ordering_key)

    def _to_proto_str(self, for_publish=False):
        """Get serialized form of ``PubsubMessage``.

        The serialized message is validated against pubsub message limits specified
        at https://cloud.google.com/pubsub/quotas#resource_limits

        Args:
            proto_msg: str containing a serialized protobuf.
            for_publish: bool, if True strip out message fields which cannot be
                published (currently message_id and publish_time) per
                https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#pubsubmessage

        Returns:
            A str containing a serialized protobuf of type
            https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#google.pubsub.v1.PubsubMessage
            containing the payload of this object.
        """
        msg = pubsub.types.PubsubMessage()
        if len(self.data) > (10_000_000):
            raise ValueError('A pubsub message data field must not exceed 10MB')
        msg.data = self.data

        if self.attributes:
            if len(self.attributes) > 100:
                raise ValueError(
                    'A pubsub message must not have more than 100 attributes.')
            for key, value in self.attributes.items():
                if len(key) > 256:
                    raise ValueError(
                        'A pubsub message attribute key must not exceed 256 bytes.')
                if len(value) > 1024:
                    raise ValueError(
                        'A pubsub message attribute value must not exceed 1024 bytes')
                msg.attributes[key] = value

        if not for_publish:
            if self.message_id:
                msg.message_id = self.message_id
                if self.publish_time:
                    msg.publish_time = self.publish_time

        if len(self.ordering_key) > 1024:
            raise ValueError(
                'A pubsub message ordering key must not exceed 1024 bytes.')
        msg.ordering_key = self.ordering_key

        serialized = pubsub.types.PubsubMessage.serialize(msg)
        if len(serialized) > (10_000_000):
            raise ValueError(
                'Serialized pubsub message exceeds the publish request limit of 10MB')
        return serialized

    @staticmethod
    def _from_message(msg: Any) -> 'PubsubMessage':
        """Construct from ``google.cloud.pubsub_v1.subscriber.message.Message``.

        https://googleapis.github.io/google-cloud-python/latest/pubsub/subscriber/api/message.html
        """
        attributes = dict((key, msg.attributes[key]) for key in msg.attributes)
        pubsubmessage = PubsubMessage(msg.data, attributes)
        if msg.message_id:
            pubsubmessage.message_id = msg.message_id
        if msg.publish_time:
            pubsubmessage.publish_time = msg.publish_time
        if msg.ordering_key:
            pubsubmessage.ordering_key = msg.ordering_key
        return pubsubmessage


class ReadFromPubSub(PTransform):
    """A ``PTransform`` for reading from Cloud Pub/Sub."""

    def __init__(
            self,
            topic: Optional[str] = None,
            subscription: Optional[str] = None,
            id_label: Optional[str] = None,
            with_attributes: bool = False,
            timestamp_attribute: Optional[str] = None) -> None:
        """Initializes ``ReadFromPubSub``.

        Args:
            topic: Optional; the topic to read from.
            subscription: Optional; the subscription to read from.
            id_label: Optional; the label for the message id.
            with_attributes: Optional; whether to include attributes.
            timestamp_attribute: Optional; the timestamp attribute.
        """
        self.topic = topic
        self.subscription = subscription
        self.id_label = id_label
        self.with_attributes = with_attributes
        self.timestamp_attribute = timestamp_attribute

    # Additional methods and logic would go here, following the same structure and logic as the Java code.
    # This is a placeholder for the rest of the implementation.
    pass  # Placeholder for additional methods and logic.