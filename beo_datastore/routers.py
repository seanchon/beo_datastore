from rest_framework.routers import DefaultRouter

from interval.views import MeterViewSet, ServiceDropViewSet
from reference.openei.views import (
    BuildingTypeViewSet,
    ReferenceBuildingViewSet,
)


v1_router = DefaultRouter()
v1_router.register(r"reference_load/building_type", BuildingTypeViewSet)
v1_router.register(
    r"reference_load/reference_building", ReferenceBuildingViewSet
)
v1_router.register(r"customer_load/meter", MeterViewSet)
v1_router.register(r"customer_load/service_drop", ServiceDropViewSet)
