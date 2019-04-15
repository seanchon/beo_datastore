from rest_framework import viewsets

from interval.models import Meter, ServiceDrop
from interval.serializers import MeterSerializer, ServiceDropSerializer


class ServiceDropViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Service drop identified by Service Address ID (SAID).
    """

    queryset = ServiceDrop.objects.all()
    serializer_class = ServiceDropSerializer


class MeterViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Meter at service drop measuring either energy import or export.
    """

    queryset = Meter.objects.all()
    serializer_class = MeterSerializer
