from django.core.exceptions import ObjectDoesNotExist
from rest_auth.serializers import (
    PasswordResetSerializer as RestAuthPasswordResetSerializer,
)
from rest_auth.registration.serializers import (
    RegisterSerializer as RestAuthRegisterSerializer,
)
from rest_framework import serializers

from beo_datastore.settings import APP_URL
from reference.auth_user.models import EmailDomain


class PasswordResetSerializer(RestAuthPasswordResetSerializer):
    """
    Overrides  ``rest_auth.serializers.PasswordResetSerializer`` to provide
    a custom password reset email template
    """

    def get_email_options(self):
        return {
            "email_template_name": "password_reset_email.html",
            "extra_email_context": {"app_url": APP_URL},
        }


class RegisterSerializer(RestAuthRegisterSerializer):
    """
    Serializer for user registration. Most of the work is managed by the
    rest_auth framework. We have additional validation to do during sign-up.
    In particular, we limit sign-ups to users with email domains that we
    recognize, and which are associated with a LSE.
    """

    def validate_email(self, email):
        """
        Validates that the provided email has a domain we are familiar with
        """
        super().validate_email(email)

        # Check for a domain that matches
        try:
            domain_str = email.split("@")[-1].lower()
            EmailDomain.objects.get(domain="@" + domain_str)
        except ObjectDoesNotExist:
            raise serializers.ValidationError(
                "Email domain is not recognized."
            )

        return email
