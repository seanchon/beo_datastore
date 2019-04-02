import io
import pandas as pd
import requests


def csv_url_to_dataframe(url):
    """
    Reads a csv from a url and returns a pandas Dataframe.

    :param url: url of csv
    :return: pandas DataFrame
    """
    csv = requests.get(url).content
    return pd.read_csv(io.StringIO(csv.decode("utf-8")))
