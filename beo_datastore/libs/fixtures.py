import inspect
import os
from shutil import rmtree
from typing import List

import django.apps
from django.conf import settings
from django.core.management import call_command

from beo_datastore.settings import MEDIA_ROOT
from beo_datastore.libs.intervalframe_file import DataFrameFile
from beo_datastore.libs.utils import mkdir_p


def load_base_fixtures_and_intervalframes():
    """
    Loads base fixtures and intervalframes.
    """
    load_fixtures_and_intervalframes("reference_model")


def load_all_fixtures_and_intervalframes():
    """
    Loads all fixtures and intervalframes.
    """
    fixture_names = [
        os.path.splitext(os.path.basename(x))[0]
        for x in get_application_fixtures(".json")
    ]
    load_fixtures_and_intervalframes(*fixture_names)


def load_fixtures_and_intervalframes(*fixture_names: List) -> None:
    """
    Load all JSON and parquet fixtures in fixture_names.

    Example:
        load_fixtures("customer", "openei", "utility_rate", "caiso_rate")
    """
    call_command("loaddata", *fixture_names)
    load_intervalframe_files()


def get_dataframe_file_models():
    """
    Return all Django models which have associated DataFrames stored to file.
    """
    return [
        x
        for x in django.apps.apps.get_models()
        if hasattr(x, "frame_file_class")
        and inspect.isclass(x.frame_file_class)
        and issubclass(x.frame_file_class, DataFrameFile)
    ]


def get_application_fixtures(extension: str) -> List:
    """
    Return the paths of all files in a subdirectory of a "fixtures/" directoy
    with a particular extension.
    """
    application_fixtures = []
    for root, dirs, files in os.walk(settings.BASE_DIR):
        if "fixtures" in root:
            for file in files:
                if file.endswith(extension):
                    application_fixtures.append(os.path.join(root, file))

    return application_fixtures


def load_intervalframe_files():
    """
    Scans all Django objects that should have associated DataFrames and loads
    parquet fixtures to MEDIA_ROOT.
    """
    parquet_fixtures = get_application_fixtures(".parquet")

    for reference_model in get_dataframe_file_models():
        frame_model = reference_model.frame_file_class
        mkdir_p(frame_model.file_directory)
        for object in reference_model.objects.all():
            filename = frame_model.get_filename(object)
            matching_files = [
                fixture for fixture in parquet_fixtures if filename in fixture
            ]
            if matching_files:
                object.frame = frame_model.get_frame_from_file(
                    reference_object=object, file_path=matching_files[0]
                )
                object.save()


def flush_intervalframe_files():
    """
    Deletes parquet fixtures from MEDIA_ROOT.
    """
    # remove empty MEDIA_ROOT
    if "media_root_test" in MEDIA_ROOT:
        try:
            rmtree(MEDIA_ROOT)
        except OSError:
            pass
