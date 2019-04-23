from beo_datastore.libs.fixtures import (
    load_all_fixtures,
    load_intervalframe_files,
)


def run(*args):
    """
    Usage:
        - python manage.py runscript beo_datastore.scripts.load_data
    """
    load_all_fixtures()
    load_intervalframe_files()
