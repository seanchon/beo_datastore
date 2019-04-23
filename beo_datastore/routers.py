from rest_framework.routers import DefaultRouter

from cost.ghg.views import GHGRateViewSet
from cost.utility_rate.views import RateCollectionViewSet, RatePlanViewSet
from load.customer.views import MeterViewSet, ServiceDropViewSet
from load.openei.views import BuildingTypeViewSet, ReferenceBuildingViewSet


v1_router = DefaultRouter()
v1_router.register(r"cost/ghg_rate", GHGRateViewSet)
v1_router.register(r"cost/utility_rate_plan", RatePlanViewSet)
v1_router.register(r"cost/utility_rate_collection", RateCollectionViewSet)
v1_router.register(r"load/openei_building_type", BuildingTypeViewSet)
v1_router.register(r"load/openei_reference_building", ReferenceBuildingViewSet)
v1_router.register(r"load/customer_meter", MeterViewSet)
v1_router.register(r"load/customer_service_drop", ServiceDropViewSet)
