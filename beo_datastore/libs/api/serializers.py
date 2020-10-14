from datetime import timedelta
import dateutil.parser
from dynamic_rest.fields import DynamicComputedField
import pandas as pd
from rest_framework import serializers

from beo_datastore.libs.models import nested_getattr


def require_request_data(request, input_list):
    """
    Require that specified Request data is passed.

    :param request: Request object
    :param input_list: required inputs (str)
    """
    missing_inputs = set(input_list) - set(request.data.keys())
    if missing_inputs:
        raise serializers.ValidationError(
            "Required: {}".format(", ".join(missing_inputs))
        )


def get_context_request_param(context, param):
    """
    Get param from context["request"]. If it does not exist, return None. POST
    operations do not have request in context, so this is a safe way to
    retrieve params.

    :param context: Serializer context
    :param param: param string
    """
    if "request" in context.keys():
        return context["request"].query_params.get(param)
    else:
        return None


class Frame288ComputedField(DynamicComputedField):
    """
    ComputedField for serializing frame 288's
    """

    frame_key: str = None

    def __init__(self, frame_key: str, **kwargs):
        self.frame_key = frame_key
        super().__init__(**kwargs)

    def get_attribute(self, instance):
        try:
            frame288 = nested_getattr(instance, self.frame_key, strict=True)
            return frame288.dataframe.astype(str)
        except AttributeError:
            raise serializers.ValidationError(
                f"Object {instance} does not have attribute {self.frame_key}"
            )


class AbstractGetDataMixin(object):
    """
    Method for serving interval data as DRF response.

    To enable, do the following inside a serializer.
    - add `data = serializers.SerializerMethodField()`
    - add "data" Meta fields.
    """

    def intervalframe_name(self):
        raise NotImplementedError(
            "intervalframe_name must be set in {}.".format(self.__class__)
        )

    def get_data(self, obj):
        """
        Used for SerializerMethodField "data". Fields for Swagger documentation
        set in MeterViewSet.schema.

        :field data_types: frame 288 type (optional)
        :field start: ISO 8601 string (optional)
        :field end_limit: ISO 8601 string (optional)
        :field period: int (optional)
        """
        data_types = get_context_request_param(self.context, "data_types")
        start = get_context_request_param(self.context, "start")
        end_limit = get_context_request_param(self.context, "end_limit")
        column = get_context_request_param(self.context, "column")
        period = get_context_request_param(self.context, "period")

        if start:
            try:
                start = dateutil.parser.parse(start)
            except Exception:
                raise serializers.ValidationError(
                    "start must be valid ISO 8601."
                )
        else:
            start = pd.Timestamp.min

        if end_limit:
            try:
                end_limit = dateutil.parser.parse(end_limit)
            except Exception:
                raise serializers.ValidationError(
                    "end_limit must be valid ISO 8601."
                )
        else:
            end_limit = pd.Timestamp.max

        if period:
            try:
                period = int(period)
            except Exception:
                raise serializers.ValidationError("period must be an integer")

        data_types = set(data_types.split(",")) if data_types else set()
        allowed_data_types = {
            "default",
            "total",
            "average",
            "maximum",
            "minimum",
            "count",
        }
        disallowed_data_types = data_types - allowed_data_types
        if disallowed_data_types:
            raise serializers.ValidationError(
                "Incorrect data_types: {}".format(
                    ", ".join(disallowed_data_types)
                )
            )

        if data_types:
            data = {}
            intervalframe = getattr(obj, self.intervalframe_name)
            intervalframe = intervalframe.filter_by_datetime(
                start=start, end_limit=end_limit
            )

            # resample the dataframe to the specified period, if one is provided
            if period:
                intervalframe = intervalframe.resample_intervalframe(
                    timedelta(minutes=period)
                )

            if column and column not in intervalframe.dataframe.columns:
                raise serializers.ValidationError(
                    "Incorrect column: {}".format(column)
                )
            elif column:  # calculate 288s on passed column
                intervalframe.aggregation_column = column

            for data_type in data_types:
                if data_type == "default":
                    dataframe = intervalframe.dataframe.reset_index()
                else:
                    frame_type = data_type + "_frame288"
                    dataframe = getattr(intervalframe, frame_type).dataframe

                data[data_type] = dataframe.where(pd.notnull(dataframe), None)

            return data
        else:
            return {}
