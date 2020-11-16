from rest_framework.routers import DefaultRouter

from cost.views import (
    CAISORateViewSet,
    GHGRateViewSet,
    ScenarioViewSet,
    RatePlanViewSet,
    RateCollectionViewSet,
    SystemProfileViewSet,
)
from der.views import (
    DERConfigurationViewSet,
    DERSimulationViewSet,
    DERStrategyViewSet,
)
from load.views import (
    CustomerClusterViewSet,
    MeterViewSet,
    MeterGroupViewSet,
    OriginFileViewSet,
)


v1_router = DefaultRouter()
v1_router.register(r"cost/caiso_rate", CAISORateViewSet, basename="CAISORate")
v1_router.register(r"cost/ghg_rate", GHGRateViewSet, basename="GHGRate")
v1_router.register(r"cost/scenario", ScenarioViewSet, basename="Scenario")
v1_router.register(r"cost/rate_plan", RatePlanViewSet, basename="RatePlan")
v1_router.register(
    r"cost/rate_collection", RateCollectionViewSet, basename="RateCollection"
)
v1_router.register(
    r"cost/system_profile", SystemProfileViewSet, basename="SystemProfile"
)
v1_router.register(
    r"der/configuration", DERConfigurationViewSet, basename="DERConfiguration"
)
v1_router.register(
    r"der/simulation", DERSimulationViewSet, basename="DERSimulation"
)
v1_router.register(r"der/strategy", DERStrategyViewSet, basename="DERStrategy")
v1_router.register(r"load/cluster", CustomerClusterViewSet)
v1_router.register(r"load/origin_file", OriginFileViewSet)
v1_router.register(r"load/meter", MeterViewSet, basename="Meter")
v1_router.register(
    r"load/meter_group", MeterGroupViewSet, basename="MeterGroup"
)
