from rest_framework import viewsets

from cost.utility_rate.models import RateCollection, RatePlan
from cost.utility_rate.serializers import (
    RateCollectionSerializer,
    RatePlanSerializer,
)


class RatePlanViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Utility Rate Plans.
    """

    queryset = RatePlan.objects.all()
    serializer_class = RatePlanSerializer


class RateCollectionViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Utility Rate Collections.
    """

    queryset = RateCollection.objects.all()
    serializer_class = RateCollectionSerializer
