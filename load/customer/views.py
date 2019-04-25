from rest_framework import viewsets

from load.customer.models import Channel, Meter
from load.customer.serializers import ChannelSerializer, MeterSerializer


class MeterViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Service drop identified by Service Address ID (SAID).
    """

    queryset = Meter.objects.all()
    serializer_class = MeterSerializer


class ChannelViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Channel at service drop measuring either energy import or export.
    """

    queryset = Channel.objects.all()
    serializer_class = ChannelSerializer
