from datetime import timedelta
import dateutil.parser
import numpy as np
import pandas as pd

from rest_framework import serializers

from load.customer.models import Meter
from load.openei.models import ReferenceBuilding
from reference.reference_model.models import MeterIntervalFrame, OriginFile


class OriginFileSerializer(serializers.HyperlinkedModelSerializer):
    filename = serializers.CharField(source="file.name")
    owners = serializers.StringRelatedField(many=True)

    class Meta:
        model = OriginFile
        fields = (
            "id",
            "uploaded_at",
            "filename",
            "owners",
            "meter_intervalframes",
        )


class MeterSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Meter
        fields = ("id", "sa_id", "rate_plan_name", "state")


class ReferenceBuildingSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = ReferenceBuilding
        fields = ("id", "location", "state", "source_file_url")


class MeterIntervalFrameSerializer(serializers.HyperlinkedModelSerializer):
    meter = MeterSerializer(many=False, read_only=True)
    referencebuilding = ReferenceBuildingSerializer(many=False, read_only=True)
    data = serializers.SerializerMethodField()

    class Meta:
        model = MeterIntervalFrame
        fields = (
            "id",
            "meter_type",
            "origin_file",
            "meter",
            "referencebuilding",
            "data",
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

        if data_types:
            data = {}
            if start:
                start = dateutil.parser.parse(start)
            else:
                start = pd.Timestamp.min

            if end_limit:
                end_limit = dateutil.parser.parse(end_limit)
            else:
                end_limit = pd.Timestamp.max

            intervalframe = obj.intervalframe.filter_by_datetime(
                start=start, end_limit=end_limit
            ).resample_intervalframe(timedelta(hours=1), np.mean)

            for data_type in data_types.split(","):
                if data_type == "default":
                    data[data_type] = intervalframe.dataframe.reset_index()
                else:
                    frame_type = data_type + "_frame288"
                    data[data_type] = getattr(
                        intervalframe, frame_type
                    ).dataframe

            return data
        else:
            return {}
