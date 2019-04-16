from rest_framework import viewsets

from cost.ghg.models import CleanNetShort
from cost.ghg.serializers import CleanNetShortSerializer


class CleanNetShortViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Clean Net Short lookup-table identified by effective date.
    """

    queryset = CleanNetShort.objects.all()
    serializer_class = CleanNetShortSerializer
