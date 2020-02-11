from datetime import timedelta
import dateutil.parser
import numpy as np
import pandas as pd

from rest_framework import serializers

from load.customer.models import CustomerMeter, OriginFile
from load.openei.models import ReferenceMeter
from reference.reference_model.models import Meter, MeterGroup


class GetDataMixin(object):
    """
    Method for serving interval data as DRF response.
    """

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
            intervalframe = obj.intervalframe.filter_by_datetime(
                start=start, end_limit=end_limit
            ).resample_intervalframe(timedelta(hours=1), np.mean)

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


class OriginFileSerializer(serializers.ModelSerializer):
    filename = serializers.CharField(source="file.name")
    owners = serializers.StringRelatedField(many=True)

    class Meta:
        model = OriginFile
        fields = ("filename", "owners")


class MeterGroupSerializer(GetDataMixin, serializers.ModelSerializer):
    originfile = OriginFileSerializer(many=False, read_only=True)
    data = serializers.SerializerMethodField()

    class Meta:
        model = MeterGroup
        fields = (
            "id",
            "created_at",
            "meter_group_type",
            "originfile",
            "meters",
            "data",
        )


class CustomerMeterSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomerMeter
        fields = ("sa_id", "rate_plan_name", "state")


class ReferenceMeterSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReferenceMeter
        fields = ("location", "state", "source_file_url")


class MeterSerializer(GetDataMixin, serializers.ModelSerializer):
    customermeter = CustomerMeterSerializer(many=False, read_only=True)
    referencemeter = ReferenceMeterSerializer(many=False, read_only=True)
    data = serializers.SerializerMethodField()

    class Meta:
        model = Meter
        fields = (
            "id",
            "meter_type",
            "meter_groups",
            "customermeter",
            "referencemeter",
            "data",
        )
