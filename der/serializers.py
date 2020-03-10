from distutils.util import strtobool

from rest_framework import serializers

from beo_datastore.libs.api.serializers import (
    AbstractGetDataMixin,
    get_context_request_param,
)
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
    data = serializers.SerializerMethodField()

    class Meta:
        model = DERConfiguration
        fields = ("id", "name", "created_at", "object_type", "data")

    def get_data(self, obj):
        """
        Nest related serializer under "data".
        """
        # allow data to be enabled
        data = get_context_request_param(self.context, "data")
        if not data or not strtobool(data):
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


class DERStrategySerializer(serializers.ModelSerializer):
    data = serializers.SerializerMethodField()

    class Meta:
        model = DERStrategy
        fields = ("id", "name", "created_at", "object_type", "data")

    def get_data(self, obj):
        """
        Nest related serializer under "data".
        """
        # allow data to be disabled
        data = get_context_request_param(self.context, "data")
        if not data or not strtobool(data):
            return {}

        if isinstance(obj, BatteryStrategy):
            return BatteryStrategySerializer(
                obj, many=False, read_only=True
            ).data
        else:
            return {}
