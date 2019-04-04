from rest_framework.routers import DefaultRouter
from rest_framework.schemas import get_schema_view

from django.conf.urls import url, include

from interval import views


schema_view = get_schema_view(title="Interval API")

# Create a router and register our viewsets with it.
router = DefaultRouter()
router.register(r"service_drop", views.ServiceDropViewSet)
router.register(r"meter", views.MeterViewSet)

# The API URLs are now determined automatically by the router.
urlpatterns = [url(r"^schema/$", schema_view), url(r"^", include(router.urls))]
