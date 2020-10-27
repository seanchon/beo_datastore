import json
import os

from navigader_core.der.solar import PVWATTS_URL

PVWATTS_FILE = os.path.join(
    "navigader_core", "tests", "test_der", "files", "pvwatts.json"
)


def mocked_pvwatts_requests_get(*args, **kwargs):
    """
    Return contents of PVWATTS_FILE on requests to PVWATTS_URL.
    """

    class MockResponse:
        def __init__(self, file_path, status_code):
            with open(file_path) as f:
                self.json_data = json.load(f)
            self.status_code = status_code

        def json(self):
            return self.json_data

    if args[0] == PVWATTS_URL:
        return MockResponse(PVWATTS_FILE, 200)

    return MockResponse(None, 404)
