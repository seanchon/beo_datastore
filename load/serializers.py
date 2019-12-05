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

    class Meta:
        model = MeterIntervalFrame
        fields = (
            "id",
            "meter_type",
            "origin_file",
            "meter",
            "referencebuilding",
        )
