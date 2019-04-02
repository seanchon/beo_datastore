import io
import os
import numpy as np
import pandas as pd
import requests


class DataFrameFile(object):
    """
    Base class that offers file-handling methods for saving/retrieving pandas
    DataFrames to/from disk.
    """

    def __init__(self, reference_object, dataframe):
        """
        :param reference_object: reference object DataFrame belongs to
        :param dataframe: pandas DataFrame
        """
        self.validate_model(reference_object)
        self.reference_object = reference_object
        self.dataframe = dataframe

    def save(self):
        """
        Saves dataframe to disk.
        """
        if not os.path.exists(self.file_directory):
            os.mkdir(self.file_directory)
        if self.reference_object.id is not None:
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
    def reference_model(self):
        """
        Set as an attribute in child class to a reference model for DataFrames.

        Ex.
            reference_model = Meter
        """
        raise NotImplementedError()

    @property
    def file_directory(self):
        """
        Set as an attribute in child class to directory where files should be
        stored.

        Ex.
            file_directory = "project_root/directory_xyz/"
        """
        raise NotImplementedError()

    @property
    def filename(self):
        return self.get_filename(self.reference_object)

    @property
    def file_path(self):
        """
        Full file path of parquet file.
        """
        return self.get_file_path(self.reference_object)

    @classmethod
    def get_filename(cls, reference_object):
        """
        Generate filename of parquet file in format
        <child>IntervalFrame.parquet.
        """
        return "{}_{}.parquet".format(cls.__name__, reference_object.id)

    @classmethod
    def get_file_path(cls, reference_object):
        """
        Generate file_path of parquet file.
        """
        return os.path.join(
            cls.file_directory, cls.get_filename(reference_object)
        )

    @classmethod
    def get_frame_from_file(cls, reference_object):
        """
        Returns IntervalFrame based on reference_object.id if it exists.

        :param reference_object: reference object IntervalFrame belongs to
        :return: pandas Frame288
        """
        file_path = cls.get_file_path(reference_object)

        if os.path.exists(file_path):
            return cls(reference_object, pd.read_parquet(file_path))
        else:
            return None

    @staticmethod
    def validate_dataframe(dataframe):
        """
        Overwrite this method for dataframe validation checks to be performed
        on every dataframe assignment to IntervalFrame.
        """
        pass

    def validate_model(self, reference_object):
        """
        Raises an Exception if reference_object is not an instance of
        self.reference_model.
        """
        if not isinstance(reference_object, self.reference_model):
            raise TypeError(
                "reference_object should be of type {}.".format(
                    self.reference_model
                )
            )

    def convert_columns_type(self, type_):
        """
        Converts columns type to another type_.
        """
        self.dataframe.columns = self.dataframe.columns.astype(type_)


class IntervalFrame(DataFrameFile):
    """
    Container class for pandas DataFrames with the following format:

    DatetimeIndex   |   value   |
    datetime        |   float   |
    """

    @staticmethod
    def validate_dataframe(dataframe):
        """
        Checks that dataframe.index is a DatetimeIndex and that a value column
        exists.
        """
        if not isinstance(
            dataframe.index, pd.core.indexes.datetimes.DatetimeIndex
        ):
            raise TypeError("DataFrame index must be DatetimeIndex.")

        if set(dataframe.columns) != {"value"}:
            raise LookupError("DataFrame must only contain value column.")

    @staticmethod
    def set_index(dataframe, index_column, convert_to_datetime=False):
        """
        Sets index on index_column and if convert_to_datetime is True, attempts
        to covert to a datetime column.

        :param dataframe: pandas DataFrame
        :param index_column: column to use as index
        :param convert_to_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        if convert_to_datetime:
            dataframe[index_column] = pd.to_datetime(dataframe[index_column])
        dataframe.set_index(index_column, inplace=True)

        return dataframe

    @classmethod
    def csv_file_to_intervalframe(
        cls,
        reference_object,
        csv_location,
        index_column=None,
        convert_to_datetime=False,
    ):
        """
        Reads a csv from file and returns an IntervalFrame.

        :param reference_object: reference object IntervalFrame belongs to
        :param csv_location: path of csv file
        :param index_column: column to use as index
        :param convert_to_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        dataframe = pd.read_csv(csv_location)
        if index_column:
            dataframe = IntervalFrame.set_index(
                dataframe, index_column, convert_to_datetime
            )

        return cls(reference_object, dataframe)

    @classmethod
    def csv_url_to_intervalframe(
        cls,
        reference_object,
        csv_url,
        index_column=None,
        convert_to_datetime=False,
    ):
        """
        Reads a csv from a url and returns an IntervalFrame.

        :param reference_object: reference object IntervalFrame belongs to
        :param csv_url: url of csv
        :param index_column: column to use as index
        :param convert_to_datetime: convert index_column to datetime if True
        :return: pandas DataFrame
        """
        csv = requests.get(csv_url).content
        dataframe = pd.read_csv(io.StringIO(csv.decode("utf-8")))
        if index_column:
            dataframe = IntervalFrame.set_index(
                dataframe, index_column, convert_to_datetime
            )

        return cls(reference_object, dataframe)

    @staticmethod
    def mask_dataframe_date(
        dataframe, year=None, month=None, day=None, hour=None, *args, **kwargs
    ):
        """
        Returns dataframe masked to match year, month, day, and/or hour.
        This method can be overwritten if self.dataframe.index is not a
        DatetimeIndex.

        :param year: integer year
        :param month: integer month
        :param day: integer day
        :param hour: integer hour
        :return: pandas DataFrame
        """
        if year is not None:
            dataframe = dataframe[dataframe.index.year == year]
        if month is not None:
            dataframe = dataframe[dataframe.index.month == month]
        if day is not None:
            dataframe = dataframe[dataframe.index.day == day]
        if hour is not None:
            dataframe = dataframe[dataframe.index.hour == hour]

        return dataframe

    def get_288_matrix(self, column, matrix_values):
        """
        Returns a 12 month by 24 hour (12 x 24 = 288) matrix where each cell is
        either the "average", "maximum", or "count" of all values in that
        particular month and hour.

        :param column: column to use for 288 matrix calculation
        :param matrix_values: choice of "average", "maximum", or "count"
        :return: pandas 12 x 24 DataFrame
        """
        frame_288 = {}

        for month in range(1, 13):
            for hour in range(0, 24):
                if month not in frame_288.keys():
                    frame_288[month] = {}

                df = self.mask_dataframe_date(
                    self.dataframe[column], month=month, hour=hour
                )

                if df.empty and matrix_values in ["average", "maximum"]:
                    frame_288[month][hour] = None
                else:
                    if matrix_values == "average":
                        frame_288[month][hour] = df.sum() / df.count()
                    elif matrix_values == "maximum":
                        frame_288[month][hour] = df.max()
                    elif matrix_values == "count":
                        frame_288[month][hour] = df.count()

        return pd.DataFrame.from_dict(frame_288)


class Frame288(DataFrameFile):
    """
    Container class for 12 x 24 pandas DataFrames with the following format:

    Int64Index  |   1       |   2       |...    |   11      |   12      |
    0           |   float   |   float   |       |   float   |   float   |
    1           |   float   |   float   |       |   float   |   float   |
    ...
    22          |   float   |   float   |       |   float   |   float   |
    23          |   float   |   float   |       |   float   |   float   |
    """

    def save(self, *args, **kwargs):
        """
        Convert columns to string on save().
        """
        self.convert_columns_type(str)
        super().save(*args, **kwargs)

    @staticmethod
    def validate_dataframe(dataframe):
        """
        Checks that dataframe.index and dataframe.columns are both an
        Int64Index. Checks that there are 12 months and 24 hours.
        """
        if not isinstance(
            dataframe.index, pd.core.indexes.numeric.Int64Index
        ) or not isinstance(
            dataframe.columns, pd.core.indexes.numeric.Int64Index
        ):
            raise TypeError("DataFrame index and columns must be Int64Index.")

        if list(dataframe.columns) != list(range(1, 13)) or list(
            dataframe.index
        ) != list(range(0, 24)):
            raise LookupError(
                "DataFrame must have an ordered index from 0 to 23 and "
                + "ordered columns from 1 to 12."
            )

    @classmethod
    def get_frame_from_file(cls, reference_object, *args, **kwargs):
        """
        Returns Frame288 based on reference_object.id if it exists. Convert
        columns to Int64 on get_frame_from_file().

        :param reference_object: reference object IntervalFrame belongs to
        :return: pandas Frame288
        """
        frame = super().get_frame_from_file(reference_object, *args, **kwargs)
        if frame:
            frame.convert_columns_type(np.int64)
        return frame
