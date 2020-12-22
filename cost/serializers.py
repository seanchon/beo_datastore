import json
from datetime import datetime, timedelta

import dateutil.parser
from dynamic_rest.fields import DynamicComputedField, DynamicRelationField
from rest_framework import serializers

from beo_datastore.libs.api.serializers import (
    BaseSerializer,
    DataField,
    IntervalFrameField,
)
from cost.ghg.models import GHGRate
from cost.procurement.models import CAISORate, SystemProfile
from cost.study.models import Scenario
from cost.utility_rate.models import RateCollection, RatePlan
from der.serializers import (
    DERConfigurationSerializer,
    DERSimulationSerializer,
    DERStrategySerializer,
)
from load.serializers import MeterGroupSerializer
from reference.auth_user.models import LoadServingEntity


class LoadServingEntitySerializer(BaseSerializer):
    class Meta:
        model = LoadServingEntity
        fields = ("id", "name", "short_name", "state")


class ScenarioSerializer(MeterGroupSerializer):
    cost_functions = serializers.SerializerMethodField()
    ders = serializers.SerializerMethodField()
    der_simulations = serializers.SerializerMethodField()
    meter_group = DynamicRelationField(MeterGroupSerializer, deferred=True)
    report = serializers.SerializerMethodField()
    report_summary = serializers.SerializerMethodField()
    der_configuration = DynamicRelationField(
        DERConfigurationSerializer, deferred=True
    )
    der_strategy = DynamicRelationField(DERStrategySerializer, deferred=True)

    class Meta:
        model = Scenario
        fields = MeterGroupSerializer.Meta.fields + (
            "cost_functions",
            "der_configuration",
            "der_simulation_count",
            "der_simulations",
            "der_strategy",
            "ders",
            "expected_der_simulation_count",
            "meter_group",
            "report",
            "report_summary",
        )
        deferred_fields = MeterGroupSerializer.Meta.deferred_fields + (
            "der_simulations",
            "ders",
            "meter_group",
            "report",
            "report_summary",
            # sorting fields
            "der_configuration",
            "der_strategy",
        )

    def get_cost_functions(self, obj):
        """
        Returns a dict with all the scenario's associated cost functions' ID and
        name
        """
        cost_functions = [
            "ghg_rate",
            "procurement_rate",
            "rate_plan",
            "system_profile",
        ]

        def serialize_cost_fn(cost_fn):
            if cost_fn is None:
                return None
            else:
                return {"id": cost_fn.id, "name": cost_fn.name}

        return {
            cost_fn: serialize_cost_fn(getattr(obj, cost_fn))
            for cost_fn in cost_functions
        }

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

    def get_metadata(self, obj: Scenario):
        """
        Data associated with Scenario child object.
        """
        return {
            "start": obj.start,
            "end_limit": obj.end_limit,
            "der_strategy": obj.der_strategy.id,
            "der_configuration": obj.der_configuration.id,
            "is_complete": obj.has_completed,
        }

    def get_report(self, obj):
        """
        Report associated with Scenario.
        """
        return json.loads(obj.report.reset_index().to_json(default_handler=str))

    def get_report_summary(self, obj):
        """
        Report summary associated with Scenario.
        """
        return json.loads(obj.report_summary.to_json(default_handler=str))


class GHGRateSerializer(BaseSerializer):
    data = serializers.SerializerMethodField()

    class Meta:
        model = GHGRate
        fields = ("data", "effective", "id", "name", "rate_unit", "source")
        deferred_fields = ("data",)

    def get_data(self, obj):
        """
        Nest GHGRate's frame288 under "data" key
        """
        [data_format, period, start, end_limit] = self._context_params(
            "data_format", "period", "start", "end_limit"
        )

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


class CAISORateSerializer(BaseSerializer):
    data = DataField()
    date_range = IntervalFrameField(source="date_range")
    filters = serializers.JSONField()
    load_serving_entity = DynamicRelationField(
        LoadServingEntitySerializer, deferred=True, embed=True
    )

    class Meta:
        model = CAISORate
        fields = (
            "id",
            "name",
            "created_at",
            "data",
            "date_range",
            "filters",
            "caiso_report",
            "load_serving_entity",
        )


class RateCollectionSerializer(BaseSerializer):
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


class EffectiveDateComputedField(DynamicComputedField):
    def __init__(self, **kwargs):
        kwargs["field_type"] = datetime
        kwargs["required"] = False
        super(EffectiveDateComputedField, self).__init__(**kwargs)

    def get_attribute(self, rate_plan):
        return rate_plan.start_date


class RatePlanSerializer(BaseSerializer):
    load_serving_entity = DynamicRelationField(
        LoadServingEntitySerializer, deferred=True
    )
    rate_collections = DynamicRelationField(
        RateCollectionSerializer, many=True, deferred=True, embed=True
    )
    start_date = EffectiveDateComputedField()

    class Meta:
        model = RatePlan
        fields = (
            "id",
            "name",
            "created_at",
            "rate_collections",
            "description",
            "demand_min",
            "demand_max",
            "load_serving_entity",
            "sector",
            "start_date",
        )


class SystemProfileSerializer(BaseSerializer):
    data = DataField()
    date_range = IntervalFrameField(source="date_range")
    load_serving_entity = DynamicRelationField(
        LoadServingEntitySerializer, deferred=True, embed=True
    )

    class Meta:
        model = SystemProfile
        fields = (
            "id",
            "name",
            "created_at",
            "load_serving_entity",
            "resource_adequacy_rate",
            "data",
            "date_range",
        )
