from rest_framework import status


class BasicAuthenticationTestMixin(object):
    """
    Tests endpoints for basic authentication restriction.
    """

    def test_anonymous_access_unauthorized(self):
        """
        Test for HTTP_403_FORBIDDEN status for anonymous access.
        """
        for endpoint in self.endpoints:
            response = self.client.get(endpoint, format="json")
            self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_user_access_ok(self):
        """
        Test for HTTP_200_OK status for logged in user access.
        """
        self.client.force_authenticate(user=self.user)

        for endpoint in self.endpoints:
            response = self.client.get(endpoint, format="json")
            self.assertEqual(response.status_code, status.HTTP_200_OK)
