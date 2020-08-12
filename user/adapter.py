from allauth.account.adapter import DefaultAccountAdapter

from beo_datastore.settings import APP_URL


class AccountAPIAdapter(DefaultAccountAdapter):
    """
    Overrides several methods of the `DefaultAccountAdapter`. These changes are
    necessary because the user does not interact directly with the API. The user
    interacts with the web application, which changes the way the server handles
    requests. For instance, the "verify account" email should provide a link for
    the uer to follow to verify their account, but the link should NOT be to the
    endpoint that performs the validation; rather, it should be to the web app's
    verification page.
    """

    def respond_email_verification_sent(self, request, user):
        """
        We don't need this redirect. The front end handles the navigation.
        """
        pass

    def get_email_confirmation_url(self, request, emailconfirmation):
        """
        Returns a link to the front end's email verification page. This is
        provided in the "verify account" email.

        Note that this tightly couples the back end and the front end in a
        fragile way. Typically this is where the Django `reverse` url utility
        would come in handy, as we we could instead write

            ```
            url = reverse("account_confirm_email", args=[emailconfirmation.key])
            ```

        and avoid duplicating the route. However, since the URL needs to point
        to the web app route, the `reverse` utility isn't able to perform that
        lookup and we're forced to explicitly specify the route.
        """
        return APP_URL + "/registration/verify/?token={token}".format(
            token=emailconfirmation.key
        )
