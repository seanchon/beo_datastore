import json

from dynamic_rest.serializers import DynamicModelSerializer
from rest_framework import serializers

from cost.study.models import SingleScenarioStudy, MultipleScenarioStudy
from der.serializers import (
    DERConfigurationSerializer,
    DERSimulationSerializer,
    DERStrategySerializer,
)
from load.serializers import MeterGroupSerializer, MeterSerializer
from reference.reference_model.models import Study


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


class StudySerializer(DynamicModelSerializer):
    ders = serializers.SerializerMethodField()
    der_simulations = serializers.SerializerMethodField()
    meter_groups = serializers.SerializerMethodField()
    meters = serializers.SerializerMethodField()
    metadata = serializers.SerializerMethodField()
    report = serializers.SerializerMethodField()

    class Meta:
        model = Study
        name = "study"
        fields = (
            "id",
            "name",
            "created_at",
            "object_type",
            "ders",
            "der_simulations",
            "meter_count",
            "meters",
            "meter_groups",
            "metadata",
            "report",
        )
        deferred_fields = (
            "ders",
            "der_simulations",
            "meters",
            "meter_groups",
            "metadata",
            "report",
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
            obj.detailed_report.reset_index().to_json(default_handler=str)
        )
