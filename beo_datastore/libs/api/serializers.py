from cached_property import cached_property
from datetime import timedelta
import dateutil.parser
from dynamic_rest.fields import DynamicComputedField
from dynamic_rest.bases import CacheableFieldMixin
from dynamic_rest.serializers import DynamicModelSerializer
import pandas as pd
from rest_framework import serializers
from typing import Dict

from beo_datastore.libs.models import nested_getattr


class ContextMixin(CacheableFieldMixin):
    """
    Mixin providing easy access to the `context` object attached to
    `DynamicModelSerializer` class instances. For some requests, the context
    dict includes the `request` object, which subsequently enables the
    serializers to customize their responses based upon parameters specified in
    the request.
    """

    @cached_property
    def __query_params(self) -> Dict[str, str]:
        request = self.context.get("request")
        return request.query_params if request else {}

    def _context_params(self, *params: str):
        """
        Get multiple query params from request context. If it does not exist,
        return None. POST operations do not have request in context, so this is
        a safe way to retrieve params.

        :param params: list of param strings, or a single string
        """
        return [self.__query_params.get(param) for param in params]

    def _context_param(self, param: str):
        """
        Get a single query param from request context. If it does not exist,
        return None. POST operations do not have request in context, so this is
        a safe way to retrieve params.

        :param param: string
        """
        return self.__query_params.get(param)


class BaseSerializer(ContextMixin, DynamicModelSerializer):
    """
    Base serializer class. This primarily exists as a convenience for
    serializers that need access to the ContextMixin
    """

    pass


class Frame288ComputedField(DynamicComputedField):
    """
    ComputedField for serializing frame 288's
    """

    frame_key: str = None

    def __init__(self, frame_key: str, *args, **kwargs):
        self.frame_key = frame_key
        super().__init__(*args, **kwargs)

    def get_attribute(self, instance):
        try:
            frame288 = nested_getattr(instance, self.frame_key, strict=True)
            return frame288.dataframe.astype(str)
        except AttributeError:
            raise serializers.ValidationError(
                f"Object {instance} does not have attribute {self.frame_key}"
            )


class DataField(ContextMixin, DynamicComputedField):
    """
    Computed field for serializing object data in a standard way
    """

    # The key used to access the instance objects' dataframes. Defaults to
    # "intervalframe"
    frame_key: str

    def __init__(self, frame_key: str = "intervalframe", *args, **kwargs):
        self.frame_key = frame_key
        super().__init__(*args, **kwargs)

    def get_attribute(self, instance):
        """
        Used for SerializerMethodField "data". Fields for Swagger documentation
        set in MeterViewSet.schema.
        """
        # If no data types were requested return early so as not to waste time
        # producing the intervalframe
        if not self.data_types:
            return {}

        data = {}
        intervalframe = self.get_intervalframe(instance)

        # For each data type, compute the dataframe to return
        for data_type in self.data_types:
            if data_type == "default":
                dataframe = intervalframe.dataframe.reset_index()
            else:
                frame_type = data_type + "_frame288"
                dataframe = getattr(intervalframe, frame_type).dataframe

            data[data_type] = dataframe.where(pd.notnull(dataframe), None)

        return data

    @cached_property
    def start(self):
        """
        Returns the `start` request param parsed, if provided. If not provided,
        returns the minimal pandas datetime. If provided but not parseable,
        raises `ValidationError`
        """
        start = self._context_param("start")
        if start:
            try:
                return dateutil.parser.parse(start)
            except Exception:
                raise serializers.ValidationError(
                    "start must be valid ISO 8601."
                )
        else:
            return pd.Timestamp.min

    @cached_property
    def end_limit(self):
        """
        Returns the `end_limit` request param parsed, if provided. If not
        provided, returns the maximal pandas datetime. If provided but not
        parseable, raises `ValidationError`
        """
        end_limit = self._context_param("end_limit")
        if end_limit:
            try:
                return dateutil.parser.parse(end_limit)
            except Exception:
                raise serializers.ValidationError(
                    "end_limit must be valid ISO 8601."
                )
        else:
            return pd.Timestamp.max

    @cached_property
    def data_types(self):
        """
        Returns a set of data types requested in the `data_types` query param.
        If any unrecognized data types are requested, throws a `ValidationError`
        """
        data_types = self._context_param("data_types")
        data_types = set(data_types.split(",")) if data_types else set()

        disallowed_data_types = data_types - {
            "default",
            "total",
            "average",
            "maximum",
            "minimum",
            "count",
        }

        if disallowed_data_types:
            raise serializers.ValidationError(
                "Incorrect data_types: {}".format(
                    ", ".join(disallowed_data_types)
                )
            )

        return data_types

    @cached_property
    def period(self):
        """
        Returns the `period` request param parsed, if provided. If not provided,
        returns None. If provided but not convertible to an int, raises
        `ValidationError`
        """
        period = self._context_param("period")
        if period:
            try:
                return int(period)
            except Exception:
                raise serializers.ValidationError("period must be an integer")

    def get_intervalframe(self, instance):
        """
        Returns the intervalframe for the instance, from which the requested
        data types can be computed. If a `period` query param was included, the
        intervalframe will be resampled to that period. If a `column` query
        param was included and is a valid column of the dataframe, that column
        will be set as the aggregation column for producing the 288 data types.

        :param instance: the model instance being accessed for its data
        """
        column = self._context_param("column")
        intervalframe = getattr(instance, self.frame_key)
        intervalframe = intervalframe.filter_by_datetime(
            start=self.start, end_limit=self.end_limit
        )

        # resample the dataframe to the specified period, if one is provided
        if self.period:
            intervalframe = intervalframe.resample_intervalframe(
                timedelta(minutes=self.period)
            )

        if column and column not in intervalframe.dataframe.columns:
            raise serializers.ValidationError(
                "Incorrect column: {}".format(column)
            )
        elif column:  # calculate 288s on passed column
            intervalframe.aggregation_column = column

        return intervalframe


class IntervalFrameField(DynamicComputedField):
    """
    Computed field for serializing an object's intervalframe, or properties
    thereof
    """

    # The key used to access the instance objects' dataframes. Defaults to
    # "intervalframe"
    frame_key: str

    # The intervalframe field to access and serialize
    source: str

    def __init__(
        self,
        frame_key: str = "intervalframe",
        source: str = None,
        *args,
        **kwargs,
    ):
        self.frame_key = frame_key
        self.source = source
        super().__init__(*args, **kwargs)

    def get_attribute(self, instance):
        intervalframe = getattr(instance, self.frame_key)
        return getattr(intervalframe, self.source)
