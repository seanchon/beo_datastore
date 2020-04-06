from dynamic_rest.fields import DynamicRelationField
from dynamic_rest.serializers import DynamicModelSerializer
from rest_framework import serializers

from beo_datastore.libs.api.serializers import AbstractGetDataMixin
from load.customer.models import CustomerMeter, OriginFile
from load.openei.models import ReferenceMeter
from reference.reference_model.models import DERSimulation, Meter, MeterGroup


class GetMeterDataMixin(AbstractGetDataMixin):
    intervalframe_name = "meter_intervalframe"


class OriginFileSerializer(DynamicModelSerializer):
    filename = serializers.CharField(source="file.name")
    owners = serializers.StringRelatedField(many=True)

    class Meta:
        model = OriginFile
        fields = ("filename", "expected_meter_count", "owners")


class MeterGroupSerializer(GetMeterDataMixin, DynamicModelSerializer):
    data = serializers.SerializerMethodField()
    meters = serializers.SerializerMethodField()
    metadata = serializers.SerializerMethodField()

    class Meta:
        model = MeterGroup
        fields = (
            "id",
            "name",
            "created_at",
            "object_type",
            "meter_count",
            "meters",
            "data",
            "metadata",
        )
        deferred_fields = ("meters",)

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
        else:
            return {}


class CustomerMeterSerializer(DynamicModelSerializer):
    class Meta:
        model = CustomerMeter
        fields = ("sa_id", "rate_plan_name", "state")


class ReferenceMeterSerializer(DynamicModelSerializer):
    class Meta:
        model = ReferenceMeter
        fields = ("location", "state", "source_file_url")


class DERSimulationSerialzier(DynamicModelSerializer):
    class Meta:
        model = DERSimulation
        fields = (
            "start",
            "end_limit",
            "meter",
            "der_configuration",
            "der_strategy",
        )


class MeterSerializer(GetMeterDataMixin, DynamicModelSerializer):
    data = serializers.SerializerMethodField()
    metadata = serializers.SerializerMethodField()
    meter_groups = DynamicRelationField("MeterGroupSerializer", many=True)

    class Meta:
        model = Meter
        fields = ("id", "object_type", "meter_groups", "data", "metadata")

    def get_metadata(self, obj):
        """
        Nest related serializer under "metadata".
        """
        if isinstance(obj, CustomerMeter):
            return CustomerMeterSerializer(
                obj, many=False, read_only=True
            ).data
        elif isinstance(obj, ReferenceMeter):
            return ReferenceMeterSerializer(
                obj, many=False, read_only=True
            ).data
        elif isinstance(obj, DERSimulation):
            return DERSimulationSerialzier(
                obj, many=False, read_only=True
            ).data
        else:
            return {}
