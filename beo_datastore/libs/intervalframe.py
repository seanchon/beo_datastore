import os
import numpy as np
import pandas as pd

from beo_datastore.libs.dataframe import csv_url_to_dataframe


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
    def default_dataframe(self):
        """
        Set as an attribute in child class to default pandas DataFrame. The
        following example is the default for an IntervalFrame:

        ex.
            default_dataframe = pd.DataFrame(
                columns=["value"],
                index=pd.to_datetime([])
            )
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
        Returns DataFrameFile based on reference_object.id if it exists.

        :param reference_object: reference object IntervalFrame belongs to
        :return: cls instance
        """
        file_path = cls.get_file_path(reference_object)

        if os.path.exists(file_path):
            return cls(reference_object, pd.read_parquet(file_path))
        else:
            return cls(reference_object, cls.default_dataframe)

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

    The following attributes can be modified for additional use cases:
    -   default_column can be updated to compute 288 summary tables on
        different columns (note: validate_dataframe will need to be updated for
        dataframes with more than one column of values).
    -   average_288_model, maximum_288_model, count_288_model can be set to
        properly-configured subclasses of Frame288 to enable file caching of
        288 summary tables.
    """

    # returns this blank dataframe if parquet file does not exist
    default_dataframe = pd.DataFrame(
        columns=["value"], index=pd.to_datetime([])
    )

    # used for average/max/count calculation, can be overwritten
    default_column = "value"

    # set to subclass of Frame288 to enable file caching
    average_288_model = None
    maximum_288_model = None
    count_288_model = None

    @property
    def average_288_dataframe(self):
        """
        Returns a 12 x 24 dataframe of average values.
        """
        if self.average_288_model is None:
            # generate frame 288
            return self.compute_288_dataframe("average")
        else:
            # retrieve from disk
            return self.read_frame_288_from_disk(
                frame_288_model=self.average_288_model
            ).dataframe

    @property
    def maximum_288_dataframe(self):
        """
        Returns a 12 x 24 dataframe of maximum values.
        """
        if self.maximum_288_model is None:
            # generate frame 288
            return self.compute_288_dataframe("maximum")
        else:
            # retrieve from disk
            return self.read_frame_288_from_disk(
                frame_288_model=self.maximum_288_model
            ).dataframe

    @property
    def count_288_dataframe(self):
        """
        Returns a 12 x 24 dataframe of counts.
        """
        if self.count_288_model is None:
            # generate frame 288
            return self.compute_288_dataframe("count")
        else:
            # retrieve from disk
            return self.read_frame_288_from_disk(
                frame_288_model=self.count_288_model
            ).dataframe

    @staticmethod
    def validate_dataframe(dataframe):
        """
        Checks that dataframe.index is a DatetimeIndex and that a single value
        column exists.
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
        Sets index on index_column. If convert_to_datetime is True, attempts
        to set index as a DatetimeIndex.

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
        dataframe = csv_url_to_dataframe(csv_url)
        if index_column:
            dataframe = IntervalFrame.set_index(
                dataframe, index_column, convert_to_datetime
            )

        return cls(reference_object, dataframe)

    def compute_288_dataframe(self, value_type):
        """
        Calculates a 12-month by 24-hour (12 x 24 = 288) dataframe where each
        cell is either the "average", "maximum", or "count" of all values in
        that particular month and hour.

        :param value_type: choice "average", "maximum", "count"
        """
        if value_type == "average":
            default_value = 0  # TODO: default to np.nan fails test
            aggfunc = np.mean
        elif value_type == "maximum":
            default_value = 0  # TODO: default to np.nan fails test
            aggfunc = np.max
        elif value_type == "count":
            default_value = 0
            aggfunc = len
        else:
            raise LookupError("Valid choices are average, maximum, and count.")

        # create a default 288
        default_288 = pd.DataFrame(
            default_value,
            columns=[x for x in range(1, 13)],
            index=[x for x in range(0, 24)],
        )

        # create summary 288 limited to self.default_column
        dataframe = self.dataframe[[self.default_column]]
        summary_288 = (
            pd.crosstab(
                dataframe.index.hour,
                dataframe.index.month,
                dataframe.values,
                aggfunc=aggfunc,
            )
            .rename_axis(None)
            .rename_axis(None, axis=1)
        )

        # merge summary 288 values into default values
        default_288.update(summary_288)

        return default_288

    def save_288_objects_to_disk(self):
        """
        Saves average/maximum/count 288 dataframes to disk to reduce future
        computation time.
        """
        for model_288, value_type in [
            (self.average_288_model, "average"),
            (self.maximum_288_model, "maximum"),
            (self.count_288_model, "count"),
        ]:
            if model_288 is not None:
                obj = model_288(
                    reference_object=self.reference_object,
                    dataframe=self.compute_288_dataframe(value_type),
                )
                obj.save()

    def read_frame_288_from_disk(self, frame_288_model):
        """
        Attempts to retieve Frame288 file from disk or generates and saves to
        disk if it does not exist.

        :param frame_288_model: subclass of Frame288
        :return: subclass of Frame288 object
        """
        frame_288 = frame_288_model.get_frame_from_file(
            reference_object=self.reference_object
        )

        # if Frame288 is not found on disk, do one-time calculation of all
        # frames and save to disk to reduce future computation time
        if frame_288.dataframe.equals(frame_288_model.default_dataframe):
            self.save_288_objects_to_disk()
            frame_288 = frame_288_model.get_frame_from_file(
                reference_object=self.reference_object
            )

        return frame_288

    def delete(self, *args, **kwargs):
        """
        Delete associated 288 objects from disk.
        """
        self.delete_288_objects_from_disk()
        super().delete(*args, **kwargs)

    def delete_288_objects_from_disk(self):
        """
        If associated 288 objects exist on disk, delete them.
        """
        for model_288 in [
            self.average_288_model,
            self.maximum_288_model,
            self.count_288_model,
        ]:
            if model_288 is not None:
                file_path = model_288.get_file_path(self.reference_object)
                if os.path.exists(file_path):
                    os.remove(file_path)


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

    # returns this blank dataframe if parquet file does not exist
    default_dataframe = pd.DataFrame(
        columns=[x for x in range(1, 13)], index=[x for x in range(0, 24)]
    )

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
        file_path = cls.get_file_path(reference_object)

        if os.path.exists(file_path):
            dataframe = pd.read_parquet(file_path)
            dataframe.columns = dataframe.columns.astype(np.int64)
            return cls(reference_object, dataframe)
        else:
            return cls(reference_object, cls.default_dataframe)
