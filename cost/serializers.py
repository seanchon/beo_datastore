import json
from datetime import timedelta, datetime
import dateutil.parser

from dynamic_rest.fields import DynamicRelationField, DynamicComputedField
from dynamic_rest.serializers import DynamicModelSerializer
from rest_framework import serializers

from beo_datastore.libs.api.serializers import (
    AbstractGetDataMixin,
    get_context_request_param,
)
from cost.ghg.models import GHGRate
from cost.procurement.models import CAISORate
from cost.study.models import Scenario
from cost.utility_rate.models import RatePlan, RateCollection
from reference.auth_user.models import LoadServingEntity
from der.serializers import (
    DERConfigurationSerializer,
    DERSimulationSerializer,
    DERStrategySerializer,
)
from load.serializers import MeterGroupSerializer, MeterSerializer


class ScenarioSerializer(AbstractGetDataMixin, DynamicModelSerializer):
    data = serializers.SerializerMethodField()
    ders = serializers.SerializerMethodField()
    der_simulations = serializers.SerializerMethodField()
    meter_group = DynamicRelationField(MeterGroupSerializer, deferred=True)
    meters = serializers.SerializerMethodField()
    metadata = serializers.SerializerMethodField()
    report = serializers.SerializerMethodField()
    report_summary = serializers.SerializerMethodField()

    # required by AbstractGetDataMixin
    intervalframe_name = "meter_intervalframe"

    class Meta:
        model = Scenario
        fields = (
            "created_at",
            "data",
            "der_simulation_count",
            "der_simulations",
            "ders",
            "expected_der_simulation_count",
            "id",
            "metadata",
            "meter_count",
            "meter_group",
            "meters",
            "name",
            "object_type",
            "report",
            "report_summary",
        )
        deferred_fields = (
            "der_simulations",
            "ders",
            "meter_group",
            "meters",
            "report",
            "report_summary",
        )

    def get_ders(self, obj):
        """
        DERs associated with Scenario.
        """
        return [
            {
                "der_configuration": DERConfigurationSerializer(
                    x["der_configuration"]
                ).data,
                "der_strategy": DERStrategySerializer(x["der_strategy"]).data,
            }
            for x in obj.ders
        ]

    def get_der_simulations(self, obj):
        """
        DERSimulations associated with Scenario.
        """
        return DERSimulationSerializer(
            obj.der_simulations, many=True, read_only=True
        ).data

    def get_meters(self, obj):
        """
        Meters associated with Scenario.
        """
        return MeterSerializer(obj.meters, many=True, read_only=True).data

    def get_meter_group(self, obj):
        """
        MeterGroups associated with Scenario.
        """
        return MeterGroupSerializer(
            obj.meter_groups, many=True, read_only=True
        ).data

    def get_metadata(self, obj: Scenario):
        """
        Data associated with Scenario child object.
        """
        return {
            "start": obj.start,
            "end_limit": obj.end_limit,
            "der_strategy": obj.der_strategy.id,
            "der_configuration": obj.der_configuration.id,
            "rate_plan_name": obj.rate_plan.name,
        }

    def get_report(self, obj):
        """
        Report associated with Scenario.
        """
        return json.loads(
            obj.report.reset_index().to_json(default_handler=str)
        )

    def get_report_summary(self, obj):
        """
        Report summary associated with Scenario.
        """
        return json.loads(obj.report_summary.to_json(default_handler=str))


class GHGRateSerializer(DynamicModelSerializer):
    data = serializers.SerializerMethodField()

    class Meta:
        model = GHGRate
        fields = ("data", "effective", "id", "name", "rate_unit", "source")
        deferred_fields = ("data",)

    def get_data(self, obj):
        """
        Nest GHGRate's frame288 under "data" key
        """
        data_format = get_context_request_param(self.context, "data_format")
        period = get_context_request_param(self.context, "period")
        start = get_context_request_param(self.context, "start")
        end_limit = get_context_request_param(self.context, "end_limit")

        if data_format == "288":
            return obj.dataframe
        elif data_format == "interval":
            if not all([start, end_limit, period]):
                raise serializers.ValidationError(
                    "start, end_limit and frequency parameters are required"
                )

            # Validate `start` parameter
            try:
                start = dateutil.parser.parse(start)
            except Exception:
                raise serializers.ValidationError(
                    "start must be valid ISO 8601."
                )

            # Validate `end_limit` parameter
            try:
                end_limit = dateutil.parser.parse(end_limit)
            except Exception:
                raise serializers.ValidationError(
                    "end_limit must be valid ISO 8601."
                )

            # Validate `frequency` parameter
            if period == "1H":
                period = timedelta(hours=1)
            elif period == "15M":
                period = timedelta(minutes=15)
            else:
                raise serializers.ValidationError(
                    "frequency parameter must be either `1H` or `15M`"
                )

            return obj.frame288.compute_intervalframe(
                start=start, end_limit=end_limit, period=period
            ).reset_index()
        else:
            return None


class CAISORateSerializer(AbstractGetDataMixin, DynamicModelSerializer):
    data = serializers.SerializerMethodField()
    filters = serializers.JSONField()
    year = serializers.SerializerMethodField()

    # required by AbstractGetDataMixin
    intervalframe_name = "intervalframe"

    class Meta:
        model = CAISORate
        fields = ("data", "filters", "id", "name", "year")

    def get_year(self, obj):
        return obj.caiso_report.year


class RateCollectionSerializer(DynamicModelSerializer):
    rate_data = serializers.JSONField()
    rate_plan = DynamicRelationField("RatePlanSerializer", deferred=True)

    class Meta:
        model = RateCollection
        fields = (
            "id",
            "rate_data",
            "effective_date",
            "openei_url",
            "utility_url",
            "rate_plan",
        )
        deferred_fields = "rate_plan"


class LoadServingEntitySerializer(DynamicModelSerializer):
    class Meta:
        model = LoadServingEntity
        fields = ("id", "name", "short_name", "state")


class EffectiveDateComputedField(DynamicComputedField):
    def __init__(self, **kwargs):
        kwargs["field_type"] = datetime
        super(EffectiveDateComputedField, self).__init__(**kwargs)

    def get_attribute(self, rate_plan):
        rate_collection = rate_plan.rate_collections.order_by(
            "effective_date"
        ).first()
        return (
            rate_collection.effective_date
            if rate_collection is not None
            else None
        )


class RatePlanSerializer(DynamicModelSerializer):
    load_serving_entity = DynamicRelationField(
        LoadServingEntitySerializer, deferred=True
    )
    rate_collections = DynamicRelationField(
        RateCollectionSerializer, many=True, deferred=True
    )
    start_date = EffectiveDateComputedField()

    class Meta:
        model = RatePlan
        fields = (
            "id",
            "name",
            "rate_collections",
            "description",
            "demand_min",
            "demand_max",
            "load_serving_entity",
            "sector",
            "start_date",
        )
