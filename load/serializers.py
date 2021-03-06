from dynamic_rest.fields import DynamicRelationField
from rest_framework import serializers

from beo_datastore.libs.api.serializers import (
    BaseSerializer,
    DataField,
    IntervalFrameField,
)
from cost.study.models import Scenario
from load.customer.models import CustomerCluster, CustomerMeter, OriginFile
from load.openei.models import ReferenceMeter
from reference.reference_model.models import DERSimulation, Meter, MeterGroup


class OriginFileSerializer(BaseSerializer):
    filename = serializers.CharField(source="file.name")

    class Meta:
        model = OriginFile
        fields = ("filename", "expected_meter_count")


class CustomerClusterSerializer(BaseSerializer):
    meter_group_id = serializers.CharField(
        source="customer_population.meter_group.id"
    )

    class Meta:
        model = CustomerCluster
        fields = (
            "cluster_type",
            "normalize",
            "cluster_id",
            "number_of_clusters",
            "meter_group_id",
        )


class ScenarioSerializer(BaseSerializer):
    is_complete = serializers.BooleanField(source="has_completed")

    class Meta:
        model = Scenario
        fields = ("is_complete",)


class MeterGroupSerializer(BaseSerializer):
    data = DataField("meter_intervalframe")
    meters = serializers.SerializerMethodField()
    metadata = serializers.SerializerMethodField()
    owners = serializers.StringRelatedField(many=True)
    date_range = IntervalFrameField(source="date_range")
    total_therms = serializers.SerializerMethodField()

    class Meta:
        model = MeterGroup
        fields = (
            "id",
            "name",
            "created_at",
            "date_range",
            "has_gas",
            "object_type",
            "meter_count",
            "owners",
            "meters",
            "data",
            "metadata",
            "max_monthly_demand",
            "total_kwh",
            "total_therms",
        )
        deferred_fields = ("meters", "total_therms")

    def get_meters(self, obj):
        """
        Used for SerializerMethodField "meters". Fields for Swagger
        documentation set in MeterViewSet.schema.

        :field meters: True or False (optional)
        """
        return MeterSerializer(obj.meters, many=True, read_only=True).data

    def get_metadata(self, obj):
        """
        Nest related serializer under "metadata".
        """
        if isinstance(obj, OriginFile):
            return OriginFileSerializer(obj, many=False, read_only=True).data
        if isinstance(obj, CustomerCluster):
            return CustomerClusterSerializer(
                obj, many=False, read_only=True
            ).data
        if isinstance(obj, Scenario):
            return ScenarioSerializer(obj, many=False, read_only=True).data
        else:
            return {}

    def get_total_therms(self, obj):
        """
        Returns the total therms associated with the MeterGroup's meters, if the
        MeterGroup is an OriginFile. Otherwise returns None.
        """
        if isinstance(obj, OriginFile):
            return obj.total_therms
        else:
            return None


class CustomerMeterSerializer(BaseSerializer):
    class Meta:
        model = CustomerMeter
        fields = ("sa_id", "rate_plan_name", "state")


class ReferenceMeterSerializer(BaseSerializer):
    class Meta:
        model = ReferenceMeter
        fields = ("location", "state", "source_file_url")


class DERSimulationSerialzier(BaseSerializer):
    class Meta:
        model = DERSimulation
        fields = (
            "start",
            "end_limit",
            "meter",
            "sa_id",
            "rate_plan_name",
            "der_configuration",
            "der_strategy",
        )


class MeterSerializer(BaseSerializer):
    data = DataField("meter_intervalframe")
    metadata = serializers.SerializerMethodField()
    meter_groups = DynamicRelationField("MeterGroupSerializer", many=True)
    total_therms = serializers.SerializerMethodField()

    class Meta:
        model = Meter
        fields = (
            "id",
            "has_gas",
            "object_type",
            "meter_groups",
            "data",
            "metadata",
            "max_monthly_demand",
            "total_kwh",
            "total_therms",
        )

    def get_metadata(self, obj):
        """
        Nest related serializer under "metadata".
        """
        if isinstance(obj, CustomerMeter):
            return CustomerMeterSerializer(obj, many=False, read_only=True).data
        elif isinstance(obj, ReferenceMeter):
            return ReferenceMeterSerializer(
                obj, many=False, read_only=True
            ).data
        elif isinstance(obj, DERSimulation):
            return DERSimulationSerialzier(obj, many=False, read_only=True).data
        else:
            return {}

    def get_total_therms(self, obj):
        """
        Returns the total therms associated with the meter, if the meter is a
        CustomerMeter. Otherwise returns None.
        """
        if isinstance(obj, CustomerMeter):
            return obj.total_therms
        else:
            return None
