import json
import pandas as pd

from cost.utility_rate.libs import (
    convert_rate_df_to_dict,
    convert_rate_dict_to_df,
)


def run(*args):
    """
    Usage:
        python manage.py runscript convert_rate_format --script-args <filename[.json|.csv]>
    """
    try:
        filename = args[0]
    except IndexError:
        print("One .csv or .json file is required")
    ext = filename.split(".")[-1]
    if ext == "csv":
        df = pd.read_csv(filename)
        print(json.dumps(convert_rate_df_to_dict(df), sort_keys=True))
    elif ext == "json":
        with open(filename) as file:
            d = json.load(file)
        if isinstance(d, list):
            raise Exception("Must be one object not a list")
        print(convert_rate_dict_to_df(d).to_csv(index=False))
