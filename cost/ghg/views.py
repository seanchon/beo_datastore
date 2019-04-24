from rest_framework import viewsets

from cost.ghg.models import GHGRate
from cost.ghg.serializers import GHGRateSerializer


class GHGRateViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Clean Net Short lookup-table identified by effective date.
    """

    queryset = GHGRate.objects.all()
    serializer_class = GHGRateSerializer
