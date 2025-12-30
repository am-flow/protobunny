import uuid

import betterproto
import pytest

import protobunny as pb_sync
from protobunny import asyncio as pb
from protobunny.backends import python as python_backend
from protobunny.helpers import get_queue
from protobunny.models import (
    MessageMixin,
    MissingRequiredFields,
    deserialize_message,
    get_message_class_from_topic,
    get_message_class_from_type_url,
    get_topic,
)

from . import tests


@pytest.fixture(autouse=True)
def setup_config(mocker, test_config) -> None:
    test_config.mode = "async"
    test_config.backend = "python"
    mocker.patch.object(pb_sync.config, "default_configuration", test_config)
    mocker.patch.object(pb_sync.models, "default_configuration", test_config)
    mocker.patch.object(pb_sync.helpers, "default_configuration", test_config)
    mocker.patch.object(pb.backends, "default_configuration", test_config)
    mocker.patch.object(python_backend.connection, "default_configuration", test_config)


def test_json_serializer() -> None:
    msg = tests.TestMessage(
        content="test",
        number=123,
        detail="test",
        options={"test": "test", "uuid": str(uuid.uuid4()), "decimal": 1.23},
    )
    serialized = bytes(msg)
    deserialized = deserialize_message(msg.topic, serialized)
    assert deserialized == msg


def test_required_fields() -> None:
    """A MissingRequiredFields exception is raised on serialization if required fields are not set."""
    with pytest.raises(MissingRequiredFields) as exc:
        _ = bytes(tests.TestMessage(detail="test"))
    assert (
        str(exc.value)
        == "Non optional fields for message acme.tests.TestMessage were not set: content, number"
    )


def test_message_classes() -> None:
    msg = tests.TestMessage(content="test", number=123)
    assert issubclass(tests.TestMessage, MessageMixin)
    assert isinstance(msg, MessageMixin)
    assert msg.topic == "acme.tests.TestMessage"
    assert msg.type_url == "tests.tests.TestMessage"


def test_get_message_class_from_type_url() -> None:
    msg = tests.TestMessage(content="test", number=123)
    msg_type = get_message_class_from_type_url(msg.type_url)
    # Assert mixin was dynamically applied
    assert issubclass(msg_type, MessageMixin)
    assert msg_type.parse is MessageMixin.parse
    assert msg_type is tests.TestMessage


def test_check_json_content_fields() -> None:
    msg = tests.TestMessage()
    assert msg.topic == "acme.tests.TestMessage"
    assert msg.json_content_fields == ["options"]


def test_message_class_from_topic() -> None:
    msg_type = get_message_class_from_topic("acme.tests.TestMessage")
    assert msg_type == tests.TestMessage
    msg_type = get_message_class_from_topic("pb.nonexisting.Message")
    assert msg_type is None


def test_get_topic() -> None:
    assert get_topic(tests.tasks.TaskMessage()) == "acme.tests.tasks.TaskMessage"
    assert get_topic(tests.TestMessage()) == "acme.tests.TestMessage"


def test_deserialize() -> None:
    message = tests.TestMessage(
        content="test", number=12, color=tests.Color.GREEN, options={"test": "123"}
    )
    serialized = bytes(message)
    topic = get_topic(message)
    deserialized = deserialize_message(topic, serialized)
    assert deserialized.color == tests.Color.GREEN
    assert deserialized == message


def test_get_queue() -> None:
    q = get_queue(tests.tasks.TaskMessage())
    assert q.shared_queue
    q = get_queue(tests.TestMessage())
    assert not q.shared_queue
    assert get_queue(tests.TestMessage()) == get_queue(tests.TestMessage)


def test_get_queue() -> None:
    q = get_queue(tests.tasks.TaskMessage())
    assert q.shared_queue
    q = get_queue(tests.TestMessage())
    assert not q.shared_queue
    assert get_queue(tests.TestMessage()) == get_queue(tests.TestMessage)


def test_to_dict() -> None:
    msg = tests.TestMessage(
        content="test", number=12, color=tests.Color.GREEN, options={"test": "123"}
    )
    to_repr = msg.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True)
    assert to_repr == dict(
        content="test", number="12", color="GREEN", options={"test": "123"}, detail=None
    )


def test_to_pydict() -> None:
    # test simple messages
    msg = tests.TestMessage(
        content="test", number=12, color=tests.Color.GREEN, options={"test": "123"}
    )
    to_repr = msg.to_pydict(casing=betterproto.Casing.SNAKE, include_default_values=True)
    assert to_repr == dict(
        content="test", number=12, color="GREEN", options={"test": "123"}, detail=None
    )
    msg = tests.TestMessage(content="test", number=12, options={"test": "123"}, detail="Test")
    to_repr = msg.to_pydict(casing=betterproto.Casing.SNAKE, include_default_values=True)
    assert to_repr == dict(
        content="test", number=12, color=None, options={"test": "123"}, detail="Test"
    )


def test_to_json() -> None:
    scan = uuid.uuid4()
    msg = tests.TestMessage(
        content="test", number=12, color=tests.Color.GREEN, options={"scan": scan}
    )
    to_repr = msg.to_json(casing=betterproto.Casing.SNAKE, include_default_values=True)
    assert (
        to_repr
        == '{"content": "test", "number": 12, "detail": null, "options": {"scan": "%s"}, "color": "GREEN"}'
        % scan
    )


def test_enum_behavior() -> None:
    """
    Test enum behavior with ser/deser
    """
    msg = tests.TestMessage(content="test", number=123, color=tests.Color.RED)
    ser = bytes(msg)
    deser = tests.TestMessage().parse(ser)
    assert deser == msg
    assert deser.color == tests.Color.RED
    assert deser.color == tests.Color.RED.value
    assert isinstance(deser.color, int)
