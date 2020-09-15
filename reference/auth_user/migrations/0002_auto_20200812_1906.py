# Generated by Django 2.2.7 on 2020-08-12 19:06

from allauth.account.models import EmailAddress
from django.contrib.auth.models import User
from django.db import migrations


def make_email_addresses(apps, schema_editor):
    """
    Creates `EmailAddress` records for users who were created via the admin
    tools
    """

    for user in User.objects.all():
        try:
            # if there's an email address for this user, continue
            EmailAddress.objects.get(user=user)
            continue
        except EmailAddress.DoesNotExist:
            # make a record for them
            EmailAddress.objects.create(
                email=user.email, primary=True, user=user, verified=True,
            )


class Migration(migrations.Migration):

    dependencies = [
        ("auth_user", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(
            make_email_addresses, reverse_code=migrations.RunPython.noop
        ),
    ]