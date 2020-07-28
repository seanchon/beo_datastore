from beo_datastore.settings import APP_URL
from rest_auth.serializers import PasswordResetSerializer


class BEOPasswordResetSerializer(PasswordResetSerializer):
    """
    Overrides  ``rest_auth.serializers.PasswordResetSerializer`` to provide
    a custom password reset email template
    """

    def get_email_options(self):
        return {
            "email_template_name": "password_reset_email.html",
            "extra_email_context": {"app_url": APP_URL},
        }
