from rest_framework.routers import DefaultRouter

from cost.views import MultipleScenarioStudyViewSet, StudyViewSet
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
v1_router.register(r"cost/study", StudyViewSet, basename="Study")
v1_router.register(
    r"cost/multiple_scenario_study", MultipleScenarioStudyViewSet
)
v1_router.register(r"der/configuration", DERConfigurationViewSet)
v1_router.register(
    r"der/simulation", DERSimulationViewSet, basename="DERSimulation"
)
v1_router.register(r"der/strategy", DERStrategyViewSet)
v1_router.register(r"load/cluster", CustomerClusterViewSet)
v1_router.register(r"load/origin_file", OriginFileViewSet)
v1_router.register(r"load/meter", MeterViewSet, basename="Meter")
v1_router.register(
    r"load/meter_group", MeterGroupViewSet, basename="MeterGroup"
)
