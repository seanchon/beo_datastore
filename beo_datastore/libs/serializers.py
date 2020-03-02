from datetime import timedelta
import dateutil.parser
import numpy as np
import pandas as pd

from rest_framework import serializers


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
        """
        data_types = self.context["request"].query_params.get("data_types")
        start = self.context["request"].query_params.get("start")
        end_limit = self.context["request"].query_params.get("end_limit")
        column = self.context["request"].query_params.get("column")

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
            ).resample_intervalframe(timedelta(hours=1), np.mean)

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
