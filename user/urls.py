from django.conf.urls import url
from rest_auth.registration.views import RegisterView, VerifyEmailView

from .views import ResendVerificationEmailView


urlpatterns = [
    url(r"^$", RegisterView.as_view(), name="rest_register"),
    url(r"^verify-email/$", VerifyEmailView.as_view(), name="verify_email"),
    url(
        r"^resend-verification/$",
        ResendVerificationEmailView.as_view(),
        name="resend_email_verification",
    ),
]
