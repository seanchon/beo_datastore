import io
import os
import pandas as pd
import requests


class IntervalFrame(object):
    def __init__(self, ref_object, dataframe):
        """
        :param ref_object: reference object
        :param dataframe: pandas DataFrame
        """
        self.ref_object = ref_object
        self.dataframe = dataframe

    def save(self):
        """
        Saves dataframe to disk.
        """
        if not os.path.exists(self.file_directory):
            os.mkdir(self.file_directory)
        self.dataframe.to_parquet(self.file_path)

    def delete(self):
        """
        Deletes dataframe from disk.
        """
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    @property
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe):
        self.validate_dataframe(dataframe)
        self._dataframe = dataframe

    @property
    def file_directory(self):
        """
        Set to return where files should be stored.
        """
        raise NotImplementedError()

    @property
    def file_prefix(self):
        """
        Overwrite to return a prefix for filename.

        Ex.
            If ref_object.id is 1 and prefix is "star_", filename would be
            "star_1.parquet"
        """
        return ""

    @property
    def filename(self):
        """
        File name of parquet file.
        """
        return "{}{}.parquet".format(self.file_prefix, self.ref_object.id)

    @property
    def file_path(self):
        """
        Full file path of parquet file.
        """
        return os.path.join(self.file_directory, self.filename)

    @classmethod
    def csv_file_to_intervalframe(
        cls, ref_object, csv_location, index_column=None, index_datetime=False
    ):
        """
        Reads a csv from file and returns an IntervalFrame.

        :param ref_object: reference object (model instance)
        :param csv_location: path of csv file
        :param index_column: column to use as index
        :index_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        dataframe = pd.read_csv(csv_location)
        if index_column:
            dataframe = IntervalFrame.set_index(
                dataframe, index_column, index_datetime
            )

        return cls(ref_object, dataframe)

    @classmethod
    def csv_url_to_intervalframe(
        cls, ref_object, csv_url, index_column=None, index_datetime=False
    ):
        """
        Reads a csv from a url and returns an IntervalFrame.

        :param ref_object: reference object (model instance)
        :param csv_url: url of csv
        :param index_column: column to use as index
        :index_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        csv = requests.get(csv_url).content
        dataframe = pd.read_csv(io.StringIO(csv.decode("utf-8")))
        if index_column:
            dataframe = IntervalFrame.set_index(
                dataframe, index_column, index_datetime
            )

        return cls(ref_object, dataframe)

    @classmethod
    def get_parquet_intervalframe(cls, ref_object):
        """
        Returns parquet IntervalFrame based on ref_object.id if it exists.
        """
        filename = "{}{}.parquet".format(cls.file_prefix, ref_object.id)
        file_path = os.path.join(cls.file_directory, filename)

        if os.path.exists(file_path):
            return cls(ref_object, pd.read_parquet(file_path))
        else:
            return None

    @staticmethod
    def set_index(dataframe, index_column, index_datetime=False):
        """
        Sets index on index_column and if index_datetime is True, attempts to
        covert to a datetime column.

        :param dataframe: pandas DataFrame
        :param index_column: column to use as index
        :index_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        if index_datetime:
            dataframe[index_column] = pd.to_datetime(dataframe[index_column])
        dataframe.set_index(index_column, inplace=True)

        return dataframe

    def validate_dataframe(self, dataframe):
        """
        Overwrite this method for dataframe validation checks to be performed
        on every dataframe assignment to IntervalFrame.
        """
        pass

    def filter_dataframe(
        self,
        column,
        year=None,
        month=None,
        day=None,
        hour=None,
        *args,
        **kwargs
    ):
        """
        Returns self.dataframe filtered by column, year, month, day, and hour.
        This method can be overwritten if self.dataframe.index is not a
        DatetimeIndex.
        """
        if column:
            dataframe = self.dataframe[[column]]
        else:
            dataframe = self.dataframe

        if year is not None:
            dataframe = dataframe[dataframe.index.year == year]
        if month is not None:
            dataframe = dataframe[dataframe.index.month == month]
        if day is not None:
            dataframe = dataframe[dataframe.index.day == day]
        if hour is not None:
            dataframe = dataframe[dataframe.index.hour == hour]

        return dataframe

    def get_288_matrix(self, column, matrix_type):
        """
        Returns a 12 month by 24 hour (12 x 24 = 288) matrix where each cell is
        either the "average", "maximum", or "count" of all values in that
        particular month and hour.

        :param column: column to use for 288 matrix calculation
        :param matrix_type: choice of "average", "maximum", or "count"
        :return: pandas 12 x 24 DataFrame
        """
        frame_288 = {}

        for month in range(1, 13):
            for hour in range(0, 24):
                if month not in frame_288.keys():
                    frame_288[month] = {}

                df = self.filter_dataframe(column, month=month, hour=hour)

                if df.empty and matrix_type in ["average", "maximum"]:
                    frame_288[month][hour] = None
                else:
                    if matrix_type == "average":
                        frame_288[month][hour] = (
                            df[column].sum() / df[column].count()
                        )
                    elif matrix_type == "maximum":
                        frame_288[month][hour] = df[column].max()
                    elif matrix_type == "count":
                        frame_288[month][hour] = df[column].count()

        return pd.DataFrame.from_dict(frame_288)
