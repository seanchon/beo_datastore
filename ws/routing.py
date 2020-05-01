from django.urls import re_path

from .consumers import ScenarioUpdatesConsumer


websocket_urlpatterns = [
    re_path(
        r"ws/%s/$" % ScenarioUpdatesConsumer.group_name,
        ScenarioUpdatesConsumer,
    ),
]
