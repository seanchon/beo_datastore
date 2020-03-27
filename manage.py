#!/usr/bin/env python
import os
import sys
import dotenv

from django.conf import settings


if __name__ == "__main__":
    dotenv_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(dotenv_file):
        dotenv.read_dotenv(dotenv_file)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "beo_datastore.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError:
        # The above import may fail for some other reason. Ensure that the
        # issue is really that Django is missing to avoid masking other
        # exceptions on Python 2.
        try:
            import django

            django
        except ImportError:
            raise ImportError(
                "Couldn't import Django. Are you sure it's installed and "
                "available on your PYTHONPATH environment variable? Did you "
                "forget to activate a virtual environment?"
            )
        raise

    # disable certain manage.py commands production
    if len(sys.argv) > 1 and sys.argv[1] in ["reset_db", "flush", "loaddata"]:
        if settings.APP_ENV == "prod":
            error_msg = "{} disabled in production".format(" ".join(sys.argv))
            raise RuntimeError(error_msg)

    execute_from_command_line(sys.argv)
