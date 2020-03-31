"""
Django settings for CEC BEO project.

Generated by 'django-admin startproject' using Django 1.11.11.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.11/ref/settings/
"""

import os
import sys

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get("SECRET_KEY")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = int(os.environ.get("DEBUG", default=0))
APP_ENV = os.environ.get("APP_ENV")

try:
    ALLOWED_HOSTS = os.environ.get("DJANGO_ALLOWED_HOSTS").split(" ")
except AttributeError:
    ALLOWED_HOSTS = []

# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.admindocs",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.sites",
    "allauth",
    "allauth.account",
    "corsheaders",
    "django_celery_beat",
    "django_celery_results",
    "django_extensions",
    "django_filters",
    "dynamic_rest",
    "polymorphic",
    "rest_auth",
    "rest_auth.registration",
    "rest_framework",
    "rest_framework.authtoken",
    "rest_framework_swagger",
    "storages",
    # apps
    "beo_datastore",
    "cost",
    "cost.ghg.apps.GhgConfig",
    "cost.procurement.apps.ProcurementConfig",
    "cost.study.apps.StudyConfig",
    "cost.utility_rate.apps.UtilityRateConfig",
    "der",
    "der.simulation.apps.SimulationConfig",
    "load",
    "load.customer.apps.CustomerConfig",
    "load.openei.apps.OpenEIConfig",
    "reference",
    "reference.reference_model.apps.ReferenceModelConfig",
    "reference.auth_user.apps.AuthUserConfig",
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django.contrib.admindocs.middleware.XViewMiddleware",
]

CORS_ORIGIN_ALLOW_ALL = False

try:
    CORS_ORIGIN_WHITELIST = os.environ.get("CORS_ORIGIN_WHITELIST").split(" ")
except AttributeError:
    CORS_ORIGIN_WHITELIST = []

ROOT_URLCONF = "beo_datastore.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ]
        },
    }
]

WSGI_APPLICATION = "beo_datastore.wsgi.application"


# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": os.environ.get("SQL_ENGINE", "django.db.backends.sqlite3"),
        "NAME": os.environ.get(
            "SQL_DATABASE", os.path.join(BASE_DIR, "db.sqlite3")
        ),
        "USER": os.environ.get("SQL_USER", "user"),
        "PASSWORD": os.environ.get("SQL_PASSWORD", "password"),
        "HOST": os.environ.get("SQL_HOST", "localhost"),
        "PORT": os.environ.get("SQL_PORT", "5432"),
    }
}


# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"
    },
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"
    },
]

# Account authentication
ACCOUNT_AUTHENTICATION_METHOD = "email"
ACCOUNT_EMAIL_REQUIRED = True
ACCOUNT_USERNAME_REQUIRED = False

# Enable registration with email instead of username
AUTHENTICATION_BACKENDS = (
    # Needed to login by username in Django admin, regardless of `allauth`
    "django.contrib.auth.backends.ModelBackend",
    # `allauth` specific authentication methods, such as login by e-mail
    "allauth.account.auth_backends.AuthenticationBackend",
)

# HTTPS
SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", False)
CSRF_COOKIE_SECURE = os.environ.get("CSRF_COOKIE_SECURE", False)

# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_L10N = True

# TODO: Set USE_TZ to True. Data ingest needs to be corrected first.
USE_TZ = False

# AWS Credentials

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_ACL = os.environ.get("AWS_DEFAULT_ACL")

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

STATIC_ROOT = STATIC_URL = os.environ.get("STATIC_ROOT", default="/static/")
AWS_STORAGE_BUCKET_NAME = os.environ.get("AWS_STORAGE_BUCKET_NAME")
if AWS_STORAGE_BUCKET_NAME:
    STATICFILES_STORAGE = "storages.backends.s3boto3.S3Boto3Storage"

# Media files (Uploads)
TESTING = len(sys.argv) > 1 and sys.argv[1] == "test"
MEDIA_ROOT_DIR = "media_root_test/" if TESTING else "media_root/"
MEDIA_ROOT = MEDIA_URL = os.path.join(
    os.environ.get("MEDIA_ROOT", BASE_DIR), MEDIA_ROOT_DIR
)
AWS_MEDIA_BUCKET_NAME = os.environ.get("AWS_MEDIA_BUCKET_NAME", "")
if AWS_MEDIA_BUCKET_NAME:
    DEFAULT_FILE_STORAGE = "beo_datastore.libs.storages.MediaStorage"

# Override Swagger's 'Django Login' Button to use DRF login page
LOGIN_URL = "rest_framework:login"
LOGOUT_URL = "rest_framework:logout"
ACCOUNT_LOGOUT_ON_GET = True

# DRF Settings
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework.authentication.SessionAuthentication",
        "rest_framework.authentication.TokenAuthentication",
    ),
    "DEFAULT_PERMISSION_CLASSES": (
        "rest_framework.permissions.IsAuthenticated",
    ),
    "DEFAULT_SCHEMA_CLASS": "rest_framework.schemas.coreapi.AutoSchema",
    "DEFAULT_FILTER_BACKENDS": [
        "django_filters.rest_framework.DjangoFilterBackend"
    ],
}

# Enable registration in django-rest-auth
SITE_ID = 1

# Django REST Swagger (Open api endpoint documentation):
SWAGGER_SETTINGS = {
    "APIS_SORTER": "alpha",
    "JSON_EDITOR": True,
    "OPERATIONS_SORTER": "method",
    "SECURITY_DEFINITIONS": {
        "api_key": {"type": "apiKey", "in": "header", "name": "Authorization"}
    },
}
if APP_ENV != "local":  # communicate over https in AWS
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

# Celery
CELERY_RESULT_BACKEND = "django-db"

CELERY_TASK_ALWAYS_EAGER = False
BROKER_URL = os.environ.get(
    "BROKER_URL", "sqs://%s:%s@" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
)
CELERY_ACCEPT_CONTENT = ["application/json"]
CELERY_RESULT_SERIALIZER = "json"
CELERY_TASK_SERIALIZER = "json"
CELERY_DEFAULT_QUEUE = os.environ.get("CELERY_DEFAULT_QUEUE", "beo_datastore")
BROKER_TRANSPORT_OPTIONS = {"region": "us-west-1", "polling_interval": 1}

TEST_RUNNER = "beo_datastore.libs.test_runner.CeleryTestSuiteRunner"

# SMTP settings
EMAIL_HOST = "email-smtp.us-west-2.amazonaws.com"
EMAIL_PORT = 587
EMAIL_HOST_USER = os.environ.get("SMTP_USER", None)
EMAIL_HOST_PASSWORD = os.environ.get("SMTP_PASSWORD", None)
EMAIL_USE_TLS = True

# django.utils.log.AdminEmailHandler configuration
SERVER_EMAIL = "support@navigader.com"  # source email
ADMINS = [("NavigaDER", "support@navigader.com")]  # destination emails

# Logging
if APP_ENV != "local":
    logging_level = "DEBUG" if APP_ENV == "dev" else "ERROR"
    LOGGING = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {"format": "%(levelname)s %(message)s"},
            "verbose": {"format": "%(asctime)s %(levelname)s %(message)s"},
        },
        "handlers": {
            "null": {"level": "DEBUG", "class": "logging.NullHandler"},
            "django_log": {
                "level": logging_level,
                "class": "logging.FileHandler",
                "filename": "/var/log/django/django.log",
                "formatter": "verbose",
            },
            "mail_admins": {
                "level": "ERROR",
                "class": "django.utils.log.AdminEmailHandler",
                "formatter": "verbose",
            },
        },
        "loggers": {
            "django": {
                "handlers": ["django_log"],
                "level": logging_level,
                "propagate": True,
            },
            "django.request": {"handlers": ["mail_admins"], "level": "ERROR"},
            "django.security.DisallowedHost": {
                "handlers": ["null"],
                "propagate": False,
            },
        },
    }
