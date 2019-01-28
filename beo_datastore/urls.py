from rest_framework_swagger.views import get_swagger_view

from django.conf.urls import url, include
from django.contrib import admin

schema_view = get_swagger_view(title="BEO Datastore")

urlpatterns = [
    url(r"^$", schema_view),
    url(r"^admin/", admin.site.urls),
    url(r"^api-auth/", include("rest_framework.urls")),
    url(r"^openei/", include("reference.openei.urls")),
    url(r"^rest-auth/", include("rest_auth.urls")),
]
