import base64
from io import BytesIO

from django.utils.html import format_html


def dataframe_to_html(dataframe):
    """
    Return dataframe as Django-formatted HTML dataframe.
    """
    return format_html(u"{}".format(dataframe.to_html()))


def plot_to_html(plt):
    """
    Return matplotlib plt as Django-formatted HTML plt.
    """
    tmpfile = BytesIO()
    plt.savefig(tmpfile, format="png")
    encoded = base64.b64encode(tmpfile.getvalue())
    plt.clf()

    return format_html(
        u"<img src='data:image/png;base64,{}'>".format(encoded.decode("utf-8"))
    )
