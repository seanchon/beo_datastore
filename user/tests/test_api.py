from allauth.account.models import EmailAddress, EmailConfirmationHMAC
from reference.auth_user.models import Profile
from rest_framework import status
from rest_framework.test import APITestCase
from django.contrib.auth.models import User


class TestRegistration(APITestCase):
    """
    Tests that the signup workflow is functional
    """

    fixtures = ["reference_model"]

    # Constants
    GOOD_PASSWORD = "sufficiently_complex_password"
    BAD_PASSWORD = "abcdefgh"

    # Endpoints
    registration_endpoint = "/rest-auth/registration/"
    verify_endpoint = "/rest-auth/registration/verify-email/"

    def register_user(
        self,
        email="jdoe@mcecleanenergy.org",
        username="john_doe",
        password1=GOOD_PASSWORD,
        password2=GOOD_PASSWORD,
    ):
        return self.client.post(
            TestRegistration.registration_endpoint,
            {
                "email": email,
                "username": username,
                "password1": password1,
                "password2": password2,
            },
        )

    @staticmethod
    def get_user(email: str = "jdoe@mcecleanenergy.org"):
        return User.objects.get(email=email)

    def test_register_with_unrecognized_email(self):
        """
        Tests that registration with an email that is unrecognized is not
        allowed
        """
        response = self.register_user(email="unrecognized@fake.com")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_register_with_recognized_email(self):
        """
        Tests that registration with an email that is recognized is OK
        """
        response = self.register_user()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # A user object should've been created, their profile should've been
        # associated with the proper LSE
        user = self.get_user()

        # email address record should be created but not verified
        email_address = EmailAddress.objects.get(user=user)
        self.assertFalse(email_address.verified)

        # profile should've been associated with the proper LSE
        profile = Profile.objects.get(user=user)
        self.assertEqual(profile.load_serving_entity.name, "MCE Clean Energy")

    def test_verify_email(self):
        """
        Tests that a user can verify their email after registering
        """
        response = self.register_user()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # User should not be verified yet
        user = self.get_user()
        email_address = EmailAddress.objects.get(user=user)
        self.assertFalse(email_address.verified)

        # Make the verification request
        response = self.client.post(
            TestRegistration.verify_endpoint,
            {"key": EmailConfirmationHMAC(email_address).key},
        )

        email_address = EmailAddress.objects.get(user=user)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(email_address.verified)
