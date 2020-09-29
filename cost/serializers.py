import json
from datetime import timedelta
import dateutil.parser

from dynamic_rest.serializers import (
    DynamicModelSerializer,
    DynamicRelationField,
)
from rest_framework import serializers

from beo_datastore.libs.api.serializers import (
    AbstractGetDataMixin,
    get_context_request_param,
)
from cost.ghg.models import GHGRate
from cost.procurement.models import CAISORate
from cost.study.models import SingleScenarioStudy, MultipleScenarioStudy
from cost.utility_rate.models import RatePlan, RateCollection
from reference.auth_user.models import LoadServingEntity
from der.serializers import (
    DERConfigurationSerializer,
    DERSimulationSerializer,
    DERStrategySerializer,
)
from load.serializers import MeterGroupSerializer, MeterSerializer
from reference.reference_model.models import Study, Sector, VoltageCategory


class GetStudyDataMixin(AbstractGetDataMixin):
    intervalframe_name = "meter_intervalframe"


class SingleScenarioStudySerializer(DynamicModelSerializer):
    rate_plan_name = serializers.CharField(source="rate_plan.name")

    class Meta:
        model = SingleScenarioStudy
        fields = (
            "id",
            "start",
            "end_limit",
            "der_strategy",
            "der_configuration",
            "rate_plan_name",
        )


class MultipleScenarioStudySerializer(DynamicModelSerializer):
    single_scenario_studies = serializers.SerializerMethodField()

    class Meta:
        model = MultipleScenarioStudy
        fields = ("id", "single_scenario_studies")

    def get_single_scenario_studies(self, obj):
        return SingleScenarioStudySerializer(
            obj.single_scenario_studies, many=True, read_only=True
        ).data


class StudySerializer(GetStudyDataMixin, DynamicModelSerializer):
    data = serializers.SerializerMethodField()
    ders = serializers.SerializerMethodField()
    der_simulations = serializers.SerializerMethodField()
    meter_groups = serializers.SerializerMethodField()
    meters = serializers.SerializerMethodField()
    metadata = serializers.SerializerMethodField()
    report = serializers.SerializerMethodField()
    report_summary = serializers.SerializerMethodField()

    class Meta:
        model = Study
        fields = (
            "id",
            "name",
            "created_at",
            "object_type",
            "der_simulation_count",
            "expected_der_simulation_count",
            "meter_count",
            "ders",
            "der_simulations",
            "meters",
            "meter_groups",
            "data",
            "metadata",
            "report",
            "report_summary",
        )
        deferred_fields = (
            "ders",
            "der_simulations",
            "meters",
            "meter_groups",
            "report",
            "report_summary",
        )

    def get_ders(self, obj):
        """
        DERs associated with Study.
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
        DERSimulations associated with Study.
        """
        return DERSimulationSerializer(
            obj.der_simulations, many=True, read_only=True
        ).data

    def get_meters(self, obj):
        """
        Meters associated with Study.
        """
        return MeterSerializer(obj.meters, many=True, read_only=True).data

    def get_meter_groups(self, obj):
        """
        MeterGroups associated with Study.
        """
        return MeterGroupSerializer(
            obj.meter_groups, many=True, read_only=True
        ).data

    def get_metadata(self, obj):
        """
        Data associated with Study child object.
        """
        if isinstance(obj, SingleScenarioStudy):
            return SingleScenarioStudySerializer(
                obj, many=False, read_only=True
            ).data
        elif isinstance(obj, MultipleScenarioStudy):
            return MultipleScenarioStudySerializer(
                obj, many=False, read_only=True
            ).data
        else:
            return {}

    def get_report(self, obj):
        """
        Report associated with Study.
        """
        return json.loads(
            obj.report.reset_index().to_json(default_handler=str)
        )

    def get_report_summary(self, obj):
        """
        Report summary associated with Study.
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


class GetCAISORateDataMixin(AbstractGetDataMixin):
    intervalframe_name = "intervalframe"


class CAISORateSerializer(GetCAISORateDataMixin, DynamicModelSerializer):
    data = serializers.SerializerMethodField()
    filters = serializers.JSONField()
    year = serializers.SerializerMethodField()

    class Meta:
        model = CAISORate
        fields = ("id", "name", "filters", "data", "year")

    def get_year(self, obj):
        return obj.caiso_report.year


class RateCollectionSerializer(DynamicModelSerializer):
    rate_data = serializers.JSONField()

    class Meta:
        model = RateCollection
        fields = (
            "id",
            "rate_data",
            "effective_date",
            "openei_url",
            "utility_url",
        )


class LoadServingEntitySerializer(DynamicModelSerializer):
    class Meta:
        model = LoadServingEntity
        fields = ("id", "name", "short_name", "state")


class SectorSerializer(DynamicModelSerializer):
    class Meta:
        model = Sector
        fields = ("id", "name", "load_serving_entity")

    load_serving_entity = DynamicRelationField(LoadServingEntitySerializer)


class VoltageCategorySerializer(DynamicModelSerializer):
    class Meta:
        model = VoltageCategory
        fields = ("id", "name", "load_serving_entity")

    load_serving_entity = DynamicRelationField(LoadServingEntitySerializer)


class RatePlanSerializer(DynamicModelSerializer):
    class Meta:
        model = RatePlan
        fields = (
            "id",
            "rate_collections",
            "description",
            "demand_min",
            "demand_max",
            "load_serving_entity",
            "sector",
            "voltage_category",
        )

    load_serving_entity = DynamicRelationField(LoadServingEntitySerializer)
    sector = DynamicRelationField(SectorSerializer)
    voltage_category = DynamicRelationField(VoltageCategorySerializer)
    rate_collections = DynamicRelationField(
        RateCollectionSerializer, many=True
    )
