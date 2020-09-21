import json
import pandas as pd

from django.test import TestCase
from django.core.files.base import ContentFile

from beo_datastore.libs.bill import (
    convert_rate_df_to_dict,
    convert_rate_dict_to_df,
)


TEST_FILE1 = "cost/utility_rate/scripts/data/openei_test_file.json"
TEST_FILE2 = (
    "cost/utility_rate/scripts/data/mce_commercial_rates_20190701.json"
)
TEST_FILE3 = (
    "cost/utility_rate/scripts/data/mce_residential_rates_20190701.json"
)


class TestRateConversion(TestCase):
    """
    For testing the conversion from OpenEI standard rate data JSON format
    to our NavigaDER templatized rate data CSV file.
    """

    def setUp(self):
        with open(TEST_FILE1) as fp:
            self.json_list = json.load(fp)
        with open(TEST_FILE2) as fp:
            self.json_list.extend(json.load(fp))
        with open(TEST_FILE3) as fp:
            self.json_list.extend(json.load(fp))

    def test_round_trip_conversion(self):
        for d in self.json_list:
            df = convert_rate_dict_to_df(d)
            file = ContentFile(df.to_csv(index=False))
            df = pd.read_csv(file)
            new_dict = convert_rate_df_to_dict(df)
            for key in new_dict:
                if "Strux" in key:
                    self.assertEqual(len(new_dict[key]), len(d[key]))
                    for idx in range(len(d[key])):
                        for tier_key in d[key][idx].keys():
                            self.assertEqual(
                                len(d[key][idx][tier_key]),
                                len(new_dict[key][idx][tier_key]),
                            )
                            for tier_idx in range(len(d[key][idx][tier_key])):
                                og_obj = d[key][idx][tier_key][tier_idx]
                                new_obj = new_dict[key][idx][tier_key][
                                    tier_idx
                                ]
                                for obj_key in og_obj:
                                    self.assertEqual(
                                        og_obj[obj_key], new_obj[obj_key]
                                    )
                else:
                    if key not in d:
                        self.assertEqual(key, "energyKeyVals")
                    else:
                        self.assertEqual(new_dict[key], d[key])
