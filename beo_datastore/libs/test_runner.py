"""
Custom test runner to allow testing of celery delayed tasks.

Copied due to package deprecation and requirements conflicts.
https://github.com/celery/django-celery/blob/master/djcelery/contrib/test_runner.py
"""


from __future__ import absolute_import, unicode_literals

from django.conf import settings

try:
    from django.test.runner import DiscoverRunner
except ImportError:
    from django.test.simple import DjangoTestSuiteRunner as DiscoverRunner

from celery import current_app


def _set_eager():
    settings.CELERY_ALWAYS_EAGER = True
    current_app.conf.CELERY_ALWAYS_EAGER = True
    settings.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True  # Issue #75
    current_app.conf.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True


class CeleryTestSuiteRunner(DiscoverRunner):
    """Django test runner allowing testing of celery delayed tasks.
    All tasks are run locally, not in a worker.
    To use this runner set ``settings.TEST_RUNNER``::
        TEST_RUNNER = "beo_datastore.libs.test_runner.CeleryTestSuiteRunner"
    """

    def setup_test_environment(self, **kwargs):
        _set_eager()
        super(CeleryTestSuiteRunner, self).setup_test_environment(**kwargs)
