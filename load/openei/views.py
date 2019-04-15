from rest_framework import viewsets

from load.openei.models import BuildingType, ReferenceBuilding
from load.openei.serializers import (
    BuildingTypeSerializer,
    ReferenceBuildingSerializer,
)


class BuildingTypeViewSet(viewsets.ReadOnlyModelViewSet):
    """
    OpenEI Commercial Reference Building Types

    Defined by: https://www.energy.gov/eere/buildings/commercial-reference-buildings
    """

    queryset = BuildingType.objects.all()
    serializer_class = BuildingTypeSerializer


class ReferenceBuildingViewSet(viewsets.ReadOnlyModelViewSet):
    """
    OpenEI Commerial and Residential Hourly Load Profiles

    Defined by: https://openei.org/doe-opendata/dataset/commercial-and-residential-hourly-load-profiles-for-all-tmy3-locations-in-the-united-states
    """

    queryset = ReferenceBuilding.objects.all()
    serializer_class = ReferenceBuildingSerializer
