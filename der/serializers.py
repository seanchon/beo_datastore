from distutils.util import strtobool

from rest_framework import serializers

from beo_datastore.libs.serializers import AbstractGetDataMixin
from der.simulation.models import BatteryConfiguration, BatteryStrategy
from reference.reference_model.models import (
    DERConfiguration,
    DERSimulation,
    DERStrategy,
)


class GetDERDataMixin(AbstractGetDataMixin):
    intervalframe_name = "der_intervalframe"


class BatteryConfigurationSerializer(serializers.ModelSerializer):
    class Meta:
        model = BatteryConfiguration
        fields = ("rating", "discharge_duration_hours", "efficiency")


class DERConfigurationSerializer(serializers.ModelSerializer):
    metadata = serializers.SerializerMethodField()

    class Meta:
        model = DERConfiguration
        fields = ("id", "name", "created_at", "object_type", "metadata")

    def get_metadata(self, obj):
        """
        Nest related serializer under "metadata".
        """
        # allow metadata to be disabled
        metadata = self.context["request"].query_params.get("metadata")
        if metadata and not strtobool(metadata):
            return {}

        if isinstance(obj, BatteryConfiguration):
            return BatteryConfigurationSerializer(
                obj, many=False, read_only=True
            ).data
        else:
            return {}


class DERSimulationSerializer(GetDERDataMixin, serializers.ModelSerializer):
    data = serializers.SerializerMethodField()

    class Meta:
        model = DERSimulation
        fields = (
            "id",
            "created_at",
            "object_type",
            "start",
            "end_limit",
            "meter",
            "der_configuration",
            "der_strategy",
            "der_columns",
            "data",
        )


class BatteryStrategySerializer(serializers.ModelSerializer):
    class Meta:
        model = BatteryStrategy
        fields = ("charge_schedule_frame", "discharge_schedule_frame")


class DERStrategySerializer(serializers.ModelSerializer):
    metadata = serializers.SerializerMethodField()

    class Meta:
        model = DERStrategy
        fields = ("id", "name", "created_at", "object_type", "metadata")

    def get_metadata(self, obj):
        """
        Nest related serializer under "metadata".
        """
        # allow metadata to be disabled
        metadata = self.context["request"].query_params.get("metadata")
        if metadata and not strtobool(metadata):
            return {}

        if isinstance(obj, BatteryStrategy):
            return BatteryStrategySerializer(
                obj, many=False, read_only=True
            ).data
        else:
            return {}
