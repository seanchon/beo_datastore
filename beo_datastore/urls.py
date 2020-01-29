from rest_framework_swagger.views import get_swagger_view

from django.conf.urls import url, include
from django.contrib import admin

from .routers import v1_router


schema_view = get_swagger_view(title="BEO Datastore")

urlpatterns = [
    url(r"^$", schema_view),
    url(r"^admin/", admin.site.urls),
    url(r"^admin/docs/", include("django.contrib.admindocs.urls")),
    url(r"^api-auth/", include("rest_framework.urls")),
    url(r"^rest-auth/", include("rest_auth.urls")),
    url(r"^v1/", include(v1_router.urls)),
]
