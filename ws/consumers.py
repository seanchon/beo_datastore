import json
from uuid import UUID
from asgiref.sync import async_to_sync
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from channels.layers import get_channel_layer

from cost.serializers import StudySerializer
from reference.reference_model.models import Study


class UUIDEncoder(json.JSONEncoder):
    """
    JSON encoder that handles UUID fields, converting them to strings
    """

    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class ScenarioUpdatesConsumer(AsyncJsonWebsocketConsumer):
    group_name = "scenario_update"

    async def connect(self):
        """
        Joins the `scenario_update` channel group
        """
        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        """
        Leaves the channel group
        """
        await self.channel_layer.group_discard(
            self.group_name, self.channel_name
        )

    async def receive_json(self, content, **kwargs):
        """
        Callback when a message is received from the WebSocket.

        :param content: the JSON-decoded payload
        """
        pass

    async def send_update(self, event):
        """
        Callback when a message is sent to the group

        :param event: the message received. Should contain an `id` parameter
        """
        scenario_id = event["id"]
        scenario = Study.objects.get(id=scenario_id)
        user = self.scope.get("user")

        # Only send the update message if the user has access to the scenario
        if user in scenario.meter_group.owners.all():
            await self.send_json(
                StudySerializer(scenario, many=False, read_only=True).data
            )

    @classmethod
    async def encode_json(cls, content):
        """
        Overrides the :AsyncJsonWebsocketConsumer.encode_json: method to
        specify the `cls` argument to the :json.dumps:

        :param content: JSON payload
        :return: string representation of the payload
        """
        return json.dumps(content, cls=UUIDEncoder)

    @classmethod
    def update_scenario(cls, scenario_id):
        """
        Sends a message to `ScenarioUpdatesConsumer`s, instructing them to
        inform WebSocket clients of an update to a scenario

        :param scenario_id: the ID of the scenario that has been updated
        """
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            cls.group_name, {"type": "send_update", "id": str(scenario_id)}
        )
