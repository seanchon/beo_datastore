from math import floor
import matplotlib.pyplot as plt

from beo_datastore.libs.intervalframe import ValidationFrame288


def plot_intervalframe(intervalframe, column=None, line_color=(0, 0, 0, 0.1)):
    """
    Plot a single graph with every daily load profile from intervalframe.

    :param intervalframe: ValidationIntervalFrame
    :param column: column to use
    :param line_color: matplotlib line color
    """
    if column is None:
        column = intervalframe.aggregation_column

    df = intervalframe.dataframe[[column]]

    for day in set(df.index.strftime("%Y-%m-%d")):
        plt.plot(df[day].index.hour, df[day][column], color=line_color)

    plt.xlim([0, 23])
    plt.xlabel("Hour of the day")
    plt.ylabel("Load")

    plt.show()


def plot_frame288(frame288, months=None, line_color=(0, 0, 0, 0.1)):
    """
    Plot a single graph with 12 monthly loads from a ValidationFrame288.

    :param frame288: ValidationFrame288
    :param months: list of months (int)
    :param line_color: matplotlib line color
    """
    if months is None:
        months = frame288.dataframe.columns

    df = frame288.dataframe

    for month in months:
        plt.plot(df[month].index, df[month], color=line_color)

    plt.xlim([0, 23])
    plt.xlabel("Hour of the day")
    plt.ylabel("Load")

    plt.show()


def plot_many_frame288s(
    frame288s,
    reference_frame288=ValidationFrame288(
        ValidationFrame288.default_dataframe
    ),
    line_color=(0, 0, 0, 0.1),
    reference_line_color="blue",
):
    """
    Plot a single graph with many ValidationFrame288s.

    :param frame288s: list of ValidationFrame288s
    :param line_color: matplotlib line color
    """
    for frame288 in frame288s:
        df = frame288.dataframe
        for month in df.columns:
            plt.plot(df[month].index, df[month], color=line_color)

    if not reference_frame288.dataframe.equals(
        ValidationFrame288.default_dataframe
    ):
        df = reference_frame288.dataframe
        for month in range(1, 13):
            plt.plot(df[month].index, df[month], color=reference_line_color)

    plt.xlim([0, 23])
    plt.xlabel("Hour of the day")
    plt.ylabel("Load")

    plt.show()


def plot_frame288_monthly_comparison(original_frame288, modified_frame288):
    """
    Plot 12 separate montly graphs of load comparisions between
    original_frame288 and modified_frame288.

    :param original_frame288: ValidationFrame288
    :param modified_frame288: ValidationFrame288
    """
    fig, axs = plt.subplots(4, 3)

    for month in range(1, 13):
        x = floor((month - 1) / 3)
        y = (month - 1) % 3
        axs[x, y].plot(
            original_frame288.dataframe[month].index,
            original_frame288.dataframe[month],
            color="blue",
        )
        axs[x, y].plot(
            modified_frame288.dataframe[month].index,
            modified_frame288.dataframe[month],
            color="green",
        )
        axs[x, y].set_title("Month {}".format(month))

    plt.show()
