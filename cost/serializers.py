from distutils.util import strtobool
import json

from rest_framework import serializers

from beo_datastore.libs.api.serializers import get_context_request_param
from cost.study.models import SingleScenarioStudy, MultipleScenarioStudy
from reference.reference_model.models import Study


class SingleScenarioStudySerializer(serializers.ModelSerializer):
    class Meta:
        model = SingleScenarioStudy
        fields = ("start", "end_limit", "der_strategy", "der_configuration")


class MultipleScenarioStudySerializer(serializers.ModelSerializer):
    class Meta:
        model = MultipleScenarioStudy
        fields = ("single_scenario_studies",)


class StudySerializer(serializers.ModelSerializer):
    der_simulations = serializers.SerializerMethodField()
    meters = serializers.SerializerMethodField()
    meter_groups = serializers.SerializerMethodField()
    metadata = serializers.SerializerMethodField()
    report = serializers.SerializerMethodField()

    class Meta:
        model = Study
        fields = (
            "id",
            "name",
            "created_at",
            "object_type",
            "der_simulations",
            "meter_count",
            "meters",
            "meter_groups",
            "metadata",
            "report",
        )

    def get_der_simulations(self, obj):
        """
        Used for SerializerMethodField "der_simulations". Fields for Swagger
        documentation set in MeterViewSet.schema.

        :field meters: True or False (optional)
        """
        ids = get_context_request_param(self.context, "ids")

        if ids and strtobool(ids):
            return obj.der_simulations.values_list("id", flat=True)
        else:
            return []

    def get_meters(self, obj):
        """
        Used for SerializerMethodField "meters". Fields for Swagger
        documentation set in MeterViewSet.schema.

        :field meters: True or False (optional)
        """
        ids = get_context_request_param(self.context, "ids")

        if ids and strtobool(ids):
            return obj.meters.values_list("id", flat=True)
        else:
            return []

    def get_meter_groups(self, obj):
        """
        Used for SerializerMethodField "meter_groups". Fields for Swagger
        documentation set in MeterViewSet.schema.

        :field meter_groups: True or False (optional)
        """
        ids = get_context_request_param(self.context, "ids")

        if ids and strtobool(ids):
            return obj.meter_groups.values_list("id", flat=True)
        else:
            return []

    def get_metadata(self, obj):
        """
        Nest related serializer under "metadata".
        """
        # allow metadata to be disabled
        metadata = get_context_request_param(self.context, "metadata")
        if metadata and not strtobool(metadata):
            return {}

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
        Nest report type under "reports".
        """
        # allow report to be disabled
        report = get_context_request_param(self.context, "report")
        if report and strtobool(report):
            return json.loads(
                obj.detailed_report.reset_index().to_json(default_handler=str)
            )
        else:
            return {}
