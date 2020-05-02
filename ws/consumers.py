import json
from uuid import UUID
from channels.generic.websocket import AsyncJsonWebsocketConsumer

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

    async def scenario_update(self, event):
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
