import functools
import json
import logging
from typing import Dict

from box.box import Box

from paho.mqtt.properties import Properties as MqttV5Properties
from paho.mqtt.packettypes import PacketTypes as MqttPacketType

from tavern._core import exceptions
from tavern._core.dict_util import check_expected_keys, format_keys
from tavern._core.extfunctions import update_from_ext
from tavern._core.pytest.config import TestConfig
from tavern._core.report import attach_yaml
from tavern._plugins.mqtt.client import MQTTClient
from tavern.request import BaseRequest

logger = logging.getLogger(__name__)


def get_publish_args(rspec: Dict, test_block_config: TestConfig) -> dict:
    """Format mqtt request args and update using ext functions"""

    fspec = format_keys(rspec, test_block_config.variables)

    if "properties" in fspec:
        publish_props = MqttV5Properties(MqttPacketType.PUBLISH)
        for prop_name, prop_value in fspec["properties"].items():
            if prop_name == 'UserProperty':
                setattr(publish_props, prop_name, tuple(prop_value))
            else:
                setattr(publish_props, prop_name, prop_value)
        fspec["properties"] = publish_props

    if "json" in fspec:
        if "payload" in fspec:
            raise exceptions.BadSchemaError(
                "Can only specify one of 'payload' or 'json' in MQTT request"
            )

        update_from_ext(fspec, ["json"])

        fspec["payload"] = json.dumps(fspec.pop("json"))

    return fspec


class MQTTRequest(BaseRequest):
    """Wrapper for a single mqtt request on a client

    Similar to RestRequest, publishes a single message.
    """

    def __init__(
        self, client: MQTTClient, rspec: Dict, test_block_config: TestConfig
    ) -> None:
        expected = {"topic", "payload", "json", "qos", "retain", "properties"}

        check_expected_keys(expected, rspec)

        if "properties" in rspec and "protocol" in client._client_args and client._client_args["protocol"] != 5:
            msg = "MQTT publish properties can only be used when the MQTT client protocol is version 5"
            logger.error(msg)
            raise exceptions.UnexpectedKeysError(msg)

        publish_args = get_publish_args(rspec, test_block_config)

        self._publish_args = publish_args
        self._prepared = functools.partial(client.publish, **publish_args)

        # Need to do this here because get_publish_args will modify the original
        # input, which we might want to use to format. No error handling because
        # all the error handling is done in the previous call
        self._original_publish_args = format_keys(rspec, test_block_config.variables)

        # TODO
        # From paho:
        # > raise TypeError('payload must be a string, bytearray, int, float or None.')
        # Need to be able to take all of these somehow, and also match these
        # against any payload received on the topic

    def run(self):
        attach_yaml(
            self._original_publish_args,
            name="rest_request",
        )

        try:
            return self._prepared()
        except ValueError as e:
            logger.exception("Error publishing")
            raise exceptions.MQTTRequestException from e

    @property
    def request_vars(self) -> Box:
        return Box(self._original_publish_args)
