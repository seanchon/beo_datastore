from dynamic_rest.serializers import DynamicModelSerializer
from rest_framework import serializers

from beo_datastore.libs.api.serializers import AbstractGetDataMixin
from der.simulation.models import BatteryConfiguration, BatteryStrategy
from reference.reference_model.models import (
    DERConfiguration,
    DERSimulation,
    DERStrategy,
)


class GetDERDataMixin(AbstractGetDataMixin):
    intervalframe_name = "der_intervalframe"


class BatteryConfigurationSerializer(DynamicModelSerializer):
    class Meta:
        model = BatteryConfiguration
        fields = ("rating", "discharge_duration_hours", "efficiency")


class DERConfigurationSerializer(DynamicModelSerializer):
    data = serializers.SerializerMethodField()

    class Meta:
        model = DERConfiguration
        fields = ("id", "name", "created_at", "object_type", "data")
        deferred_fields = ("data",)

    def get_data(self, obj):
        """
        Nest related serializer under "data".
        """
        if isinstance(obj, BatteryConfiguration):
            return BatteryConfigurationSerializer(
                obj, many=False, read_only=True
            ).data
        else:
            return {}


class DERSimulationSerializer(GetDERDataMixin, DynamicModelSerializer):
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


class BatteryStrategySerializer(DynamicModelSerializer):
    charge_schedule_frame = serializers.SerializerMethodField()
    discharge_schedule_frame = serializers.SerializerMethodField()

    class Meta:
        model = BatteryStrategy
        fields = ("charge_schedule_frame", "discharge_schedule_frame")

    def get_charge_schedule_frame(self, obj):
        """
        Convert float("inf") and float("-inf") to string representations.
        """
        return obj.charge_schedule.frame288.dataframe.astype(str)

    def get_discharge_schedule_frame(self, obj):
        """
        Convert float("inf") and float("-inf") to string representations.
        """
        return obj.discharge_schedule.frame288.dataframe.astype(str)


class DERStrategySerializer(DynamicModelSerializer):
    data = serializers.SerializerMethodField()

    class Meta:
        model = DERStrategy
        fields = ("id", "name", "created_at", "object_type", "data")
        deferred_fields = ("data",)

    def get_data(self, obj):
        """
        Nest related serializer under "data".
        """
        if isinstance(obj, BatteryStrategy):
            return BatteryStrategySerializer(
                obj, many=False, read_only=True
            ).data
        else:
            return {}
