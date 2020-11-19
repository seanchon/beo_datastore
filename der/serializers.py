from dynamic_rest.fields import DynamicRelationField
from rest_framework import serializers

from beo_datastore.libs.api.serializers import (
    BaseSerializer,
    DataField,
    Frame288ComputedField,
)
from der.simulation.models import (
    BatteryConfiguration,
    BatteryStrategy,
    EVSEConfiguration,
    EVSEStrategy,
    SolarPVConfiguration,
    SolarPVStrategy,
)
from reference.reference_model.models import (
    DERConfiguration,
    DERSimulation,
    DERStrategy,
)


class BatteryConfigurationSerializer(BaseSerializer):
    class Meta:
        model = BatteryConfiguration
        fields = ("rating", "discharge_duration_hours", "efficiency")


class EVSEConfigurationSerializer(BaseSerializer):
    class Meta:
        model = EVSEConfiguration
        fields = (
            "ev_mpkwh",
            "ev_mpg_eq",
            "ev_capacity",
            "ev_efficiency",
            "evse_rating",
            "ev_count",
            "evse_count",
        )


class SolarPVConfigurationSerializer(BaseSerializer):
    address = serializers.CharField(source="parameters.address")
    array_type = serializers.IntegerField(source="parameters.array_type")
    azimuth = serializers.FloatField(source="parameters.azimuth")
    tilt = serializers.FloatField(source="parameters.tilt")
    losses = serializers.FloatField(source="parameters.losses")
    system_capacity = serializers.FloatField(
        source="parameters.system_capacity"
    )

    class Meta:
        model = SolarPVConfiguration
        fields = (
            "address",
            "address",
            "array_type",
            "azimuth",
            "tilt",
            "losses",
            "system_capacity",
        )


class DERConfigurationSerializer(BaseSerializer):
    data = serializers.SerializerMethodField()

    class Meta:
        model = DERConfiguration
        fields = (
            "id",
            "name",
            "created_at",
            "object_type",
            "data",
            "der_type",
        )
        deferred_fields = ("data",)

    def get_data(self, obj):
        """
        Nest related serializer under "data".
        """
        if isinstance(obj, BatteryConfiguration):
            serializer = BatteryConfigurationSerializer
        elif isinstance(obj, EVSEConfiguration):
            serializer = EVSEConfigurationSerializer
        elif isinstance(obj, SolarPVConfiguration):
            serializer = SolarPVConfigurationSerializer
        else:
            return {}
        return serializer(obj, many=False, read_only=True).data


class DERSimulationSerializer(BaseSerializer):
    data = DataField("der_intervalframe")
    meter = DynamicRelationField("load.serializers.MeterSerializer")

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


class BatteryStrategySerializer(BaseSerializer):
    charge_schedule_frame = Frame288ComputedField("charge_schedule.frame288")
    discharge_schedule_frame = Frame288ComputedField(
        "discharge_schedule.frame288"
    )

    class Meta:
        model = BatteryStrategy
        fields = ("charge_schedule_frame", "discharge_schedule_frame")


class EVSEStrategySerializer(BaseSerializer):
    charge_schedule = Frame288ComputedField("charge_schedule.frame288")
    drive_schedule = Frame288ComputedField("drive_schedule.frame288")

    class Meta:
        model = EVSEStrategy
        fields = ("charge_schedule", "drive_schedule")


class SolarPVStrategySerializer(BaseSerializer):
    serviceable_load_ratio = serializers.FloatField(
        source="parameters.serviceable_load_ratio"
    )

    class Meta:
        model = SolarPVStrategy
        fields = ("serviceable_load_ratio",)


class DERStrategySerializer(BaseSerializer):
    data = serializers.SerializerMethodField()

    class Meta:
        model = DERStrategy
        fields = (
            "id",
            "name",
            "description",
            "created_at",
            "object_type",
            "objective",
            "data",
            "der_type",
        )
        deferred_fields = ("data",)

    def get_data(self, obj):
        """
        Nest related serializer under "data".
        """
        if isinstance(obj, BatteryStrategy):
            serializer = BatteryStrategySerializer
        elif isinstance(obj, EVSEStrategy):
            serializer = EVSEStrategySerializer
        elif isinstance(obj, SolarPVStrategy):
            serializer = SolarPVStrategySerializer
        else:
            return {}
        return serializer(obj, many=False, read_only=True).data
