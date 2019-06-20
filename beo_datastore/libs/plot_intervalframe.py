from math import floor
import matplotlib.pyplot as plt

from beo_datastore.libs.intervalframe import ValidationFrame288


def plot_intervalframe(
    intervalframe, column=None, line_color=(0, 0, 0, 0.1), save_as=None
):
    """
    Plot a single graph with every daily load profile from intervalframe.

    :param intervalframe: ValidationIntervalFrame
    :param column: column to use
    :param line_color: matplotlib line color
    :param save_as: destination to save PNG file
    """
    if column is None:
        column = intervalframe.aggregation_column

    df = intervalframe.dataframe[[column]]

    for day in set(df.index.strftime("%Y-%m-%d")):
        plt.plot(df[day].index.hour, df[day][column], color=line_color)

    plt.xlim([0, 23])
    plt.xlabel("Hour of the day")
    plt.ylabel("Load")

    plt.tight_layout()
    if save_as:
        plt.savefig(save_as, format="png", dpi=1000)
    plt.show()


def plot_frame288(
    frame288, months=None, line_color=(0, 0, 0, 0.1), save_as=None
):
    """
    Plot a single graph with 12 monthly loads from a ValidationFrame288.

    :param frame288: ValidationFrame288
    :param months: list of months (int)
    :param line_color: matplotlib line color
    :param save_as: destination to save PNG file
    """
    if months is None:
        months = frame288.dataframe.columns

    df = frame288.dataframe

    for month in months:
        plt.plot(df[month].index, df[month], color=line_color)

    plt.xlim([0, 23])
    plt.xlabel("Hour of the day")
    plt.ylabel("Load")

    plt.tight_layout()
    if save_as:
        plt.savefig(save_as, format="png", dpi=1000)
    plt.show()


def plot_many_frame288s(
    frame288s,
    reference_frame288=ValidationFrame288(
        ValidationFrame288.default_dataframe
    ),
    months=None,
    line_color=(0, 0, 0, 0.1),
    reference_line_color="blue",
    save_as=None,
):
    """
    Plot many ValidationFrame288s with an optional reference
    ValidationFrame288.

    :param frame288s: list of ValidationFrame288s
    :param line_color: matplotlib line color
    :param save_as: destination to save PNG file
    """
    if months is None:
        months = frame288s[0].dataframe.columns

    for frame288 in frame288s:
        df = frame288.dataframe
        for month in months:
            plt.plot(df[month].index, df[month], color=line_color)

    if not reference_frame288.dataframe.equals(
        ValidationFrame288.default_dataframe
    ):
        df = reference_frame288.dataframe
        for month in months:
            plt.plot(df[month].index, df[month], color=reference_line_color)

    plt.xlim([0, 23])
    plt.xlabel("Hour of the day")
    plt.ylabel("Load")

    plt.tight_layout()
    if save_as:
        plt.savefig(save_as, format="png", dpi=1000)
    plt.show()


def plot_frame288_monthly_comparison(
    original_frame288,
    modified_frame288,
    original_line_color="grey",
    modified_line_color="blue",
    save_as=None,
):
    """
    Plot 12 separate montly graphs of load comparisions between
    original_frame288 and modified_frame288.

    :param original_frame288: ValidationFrame288
    :param modified_frame288: ValidationFrame288
    :param save_as: destination to save PNG file
    """
    fig, axs = plt.subplots(4, 3)

    for month in range(1, 13):
        x = floor((month - 1) / 3)
        y = (month - 1) % 3
        axs[x, y].plot(
            original_frame288.dataframe[month].index,
            original_frame288.dataframe[month],
            color=original_line_color,
        )
        axs[x, y].plot(
            modified_frame288.dataframe[month].index,
            modified_frame288.dataframe[month],
            color=modified_line_color,
        )
        axs[x, y].set_title("Month {}".format(month))

    plt.tight_layout()
    if save_as:
        plt.savefig(save_as, format="png", dpi=1000)
    plt.show()
