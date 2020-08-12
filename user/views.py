from allauth.account.utils import send_email_confirmation

from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist

from rest_framework import serializers, status
from rest_framework.generics import GenericAPIView
from rest_framework.permissions import AllowAny
from rest_framework.response import Response


class ResendVerificationEmailView(GenericAPIView):
    """
    Basic view that takes a POST request with an email parameter and re-sends an
    email to verify a user's account.
    """

    permission_classes = (AllowAny,)

    def post(self, request, *args, **kwargs):
        email = request.data.get("email")

        try:
            user = User.objects.get(email=email)
        except ObjectDoesNotExist:
            raise serializers.ValidationError(
                "User with email {} does not exist".format(email)
            )

        # Send the email
        send_email_confirmation(request, user)
        return Response(
            {"detail": "Account verification email has been sent"},
            status=status.HTTP_200_OK,
        )
