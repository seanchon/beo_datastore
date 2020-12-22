import os

from django.conf.urls import include, url
from django.contrib import admin
from rest_auth.views import PasswordResetView
from rest_framework_swagger.views import get_swagger_view

from user.serializers import PasswordResetSerializer
from .routers import v1_router

schema_view = get_swagger_view(title="BEO Datastore")

# Provide a different name for the admin via `ADMIN_URL` environment variable
# to prevent attackers from easily profiling the site.
hard_to_guess_admin = os.environ.get("ADMIN_URL", default="admin")
hard_to_guess_admin = hard_to_guess_admin.strip().replace("/", "").lower()

urlpatterns = [
    url(r"^$", schema_view),
    url(r"^{}/".format(hard_to_guess_admin), admin.site.urls),
    url(r"^api-auth/", include("rest_framework.urls")),
    url(r"^rest-auth/registration/", include("user.urls")),
    url(
        r"^rest-auth/password/reset/$",
        PasswordResetView.as_view(serializer_class=PasswordResetSerializer),
    ),
    url(r"^rest-auth/", include("rest_auth.urls")),
    url(r"^v1/", include(v1_router.urls)),
]
