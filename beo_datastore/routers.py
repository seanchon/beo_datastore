from rest_framework.routers import DefaultRouter

from cost.ghg.views import GHGRateViewSet
from cost.utility_rate.views import RateCollectionViewSet, RatePlanViewSet


v1_router = DefaultRouter()
v1_router.register(r"cost/ghg_rate", GHGRateViewSet)
v1_router.register(r"cost/utility_rate_plan", RatePlanViewSet)
v1_router.register(r"cost/utility_rate_collection", RateCollectionViewSet)
