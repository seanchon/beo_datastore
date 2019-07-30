from beo_datastore.libs.fixtures import (
    load_base_fixtures,
    load_test_fixtures,
    load_intervalframe_files,
)


def run(*args):
    """
    Usage:
        - python manage.py runscript beo_datastore.scripts.load_data
    """
    load_base_fixtures()
    if len(args) > 0 and args[0] == "test":
        load_test_fixtures()
    load_intervalframe_files()
