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


def convert_columns_type(dataframe, type_):
    """
    Converts columns type to another type_.
    """
    dataframe.columns = dataframe.columns.astype(type_)
    return dataframe
