from rest_framework import permissions, viewsets

from beo_datastore.libs.permissions import IsOwnerOrReadOnly

from reference.openei.models import BuildingType, ReferenceBuilding
from reference.openei.serializers import (
    BuildingTypeSerializer,
    ReferenceBuildingSerializer,
)


class BuildingTypeViewSet(viewsets.ReadOnlyModelViewSet):
    """
    OpenEI Commercial Reference Buildings

    Defined by: https://www.energy.gov/eere/buildings/commercial-reference-buildings
    """

    queryset = BuildingType.objects.all()
    serializer_class = BuildingTypeSerializer
    permission_classes = (
        permissions.IsAuthenticatedOrReadOnly,
        IsOwnerOrReadOnly,
    )


class ReferenceBuildingViewSet(viewsets.ReadOnlyModelViewSet):
    """
    OpenEI Commerial and Residential Hourly Load Profiles

    Defined by: https://openei.org/doe-opendata/dataset/commercial-and-residential-hourly-load-profiles-for-all-tmy3-locations-in-the-united-states
    """

    queryset = ReferenceBuilding.objects.all()
    serializer_class = ReferenceBuildingSerializer
    permission_classes = (
        permissions.IsAuthenticatedOrReadOnly,
        IsOwnerOrReadOnly,
    )
