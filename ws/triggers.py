from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from .consumers import ScenarioUpdatesConsumer


def update_scenario(scenario_id: str):
    """
    Sends a message to `ScenarioUpdatesConsumer`s, instructing them to inform
    WebSocket clients of an update to a scenario

    :param scenario_id: the ID of the scenario that has been updated
    """
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        ScenarioUpdatesConsumer.group_name,
        {"type": "scenario_update", "id": str(scenario_id)},
    )
