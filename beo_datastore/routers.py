from rest_framework.routers import DefaultRouter

from cost.ghg.views import CleanNetShortViewSet
from load.customer.views import MeterViewSet, ServiceDropViewSet
from load.openei.views import BuildingTypeViewSet, ReferenceBuildingViewSet


v1_router = DefaultRouter()
v1_router.register(r"cost/ghg_clean_net_short", CleanNetShortViewSet)
v1_router.register(r"load/openei_building_type", BuildingTypeViewSet)
v1_router.register(r"load/openei_reference_building", ReferenceBuildingViewSet)
v1_router.register(r"load/customer_meter", MeterViewSet)
v1_router.register(r"load/customer_service_drop", ServiceDropViewSet)
