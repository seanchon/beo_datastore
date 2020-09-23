import coreapi
from functools import reduce
import pandas as pd

from django.db import transaction

from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.schemas import AutoSchema

from beo_datastore.libs.api.serializers import require_request_data
from beo_datastore.libs.api.viewsets import (
    CreateViewSet,
    ListRetrieveUpdateDestroyViewSet,
    ListRetrieveViewSet,
)
from beo_datastore.libs.dataframe import download_dataframe
from beo_datastore.libs.models import get_exact_many_to_many, nested_getattr

from cost.ghg.models import GHGRate
from cost.procurement.models import CAISORate, SystemProfile
from cost.study.models import SingleScenarioStudy, MultipleScenarioStudy
from cost.utility_rate.models import RatePlan
from reference.reference_model.models import (
    DERConfiguration,
    DERStrategy,
    MeterGroup,
    Study,
)

from .serializers import (
    CAISORateSerializer,
    GHGRateSerializer,
    MultipleScenarioStudySerializer,
    StudySerializer,
)
from .tasks import run_study


class MultipleScenarioStudyViewSet(CreateViewSet):
    """
    Studies containing multiple SingleScenarioStudies.
    """

    queryset = MultipleScenarioStudy.objects.all()
    serializer_class = MultipleScenarioStudySerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "name",
                required=True,
                location="body",
                description=("MultipleScenarioStudy Name."),
            ),
            coreapi.Field(
                "meter_group_ids",
                required=True,
                location="body",
                description=(
                    "JSON List of MeterGroup ids. "
                    "Ex. ['<meter_group_id>', '<meter_group_id>']"
                ),
            ),
            coreapi.Field(
                "ders",
                required=True,
                location="body",
                description=(
                    "JSON List of der_configuration_id, der_strategy_id pairs. "
                    "Ex. [{'der_configuration_id': '<id>', "
                    "'der_strategy_id': '<id>'}, ]"
                ),
            ),
            coreapi.Field(
                "rate_plan_id",
                required=False,
                location="body",
                description=("RatePlan id to use for billing calculations."),
            ),
            coreapi.Field(
                "object_type",
                required=False,
                location="query",
                description=("Filter by object_type field."),
            ),
        ]
    )

    def create(self, request):
        require_request_data(request, ["name", "meter_group_ids", "ders"])

        name = request.data["name"]
        meter_group_ids = request.data["meter_group_ids"]
        ders = request.data["ders"]

        with transaction.atomic():
            single_scenario_study_ids = set()
            for meter_group_id in meter_group_ids:
                for der in ders:
                    configuration_id = der["der_configuration_id"]
                    strategy_id = der["der_strategy_id"]
                    try:
                        meter_group = MeterGroup.objects.get(id=meter_group_id)
                    except MeterGroup.DoesNotExist:
                        raise serializers.ValidationError(
                            "MeterGroup does not exist."
                        )
                    if "rate_plan_id" in request.data.keys():
                        rate_plan = RatePlan.objects.get(
                            id=request.data["rate_plan_id"]
                        )
                    else:
                        rate_plan = RatePlan.get_linked_rate_plans(
                            meter_group.load_serving_entity,
                            meter_group.primary_linked_rate_plan_name,
                        ).first()
                        if not rate_plan:
                            raise serializers.ValidationError(
                                "Could not determine RatePlan for MeterGroup "
                                "(name: {}, id: {}).".format(
                                    meter_group.name, meter_group.id
                                )
                            )
                    try:
                        der_configuration = DERConfiguration.objects.get(
                            id=configuration_id
                        )
                    except DERConfiguration.DoesNotExist:
                        raise serializers.ValidationError(
                            "DERConfiguration does not exist."
                        )
                    try:
                        der_strategy = DERStrategy.objects.get(id=strategy_id)
                    except DERStrategy.DoesNotExist:
                        raise serializers.ValidationError(
                            "DERStrategy does not exist."
                        )

                    single, _ = SingleScenarioStudy.objects.get_or_create(
                        start=pd.Timestamp.min,
                        # Bug: Django rounds pd.Timestamp.max up on save
                        end_limit=pd.Timestamp.max.replace(microsecond=0),
                        der_configuration=der_configuration,
                        der_strategy=der_strategy,
                        meter_group=meter_group,
                        rate_plan=rate_plan,
                        name=name,
                    )
                    single.ghg_rates.add(
                        *GHGRate.objects.filter(
                            name__contains="Clean Net Short"
                        )
                    )
                    single.ghg_rates.add(
                        *GHGRate.objects.filter(name__contains="CARB")
                    )
                    lse = request.user.profile.load_serving_entity
                    # TODO: Account for CCA's with multiple system profiles
                    system_profile = SystemProfile.objects.filter(
                        load_serving_entity=lse
                    ).last()
                    if system_profile:
                        single.system_profiles.add(system_profile)

                    # assign CAISO rates
                    caiso_rates = CAISORate.objects.filter(
                        caiso_report__report_name="PRC_LMP",
                        caiso_report__year__in=meter_group.years,
                    )
                    parent_utility = nested_getattr(
                        meter_group, "load_serving_entity.parent_utility.name"
                    )
                    if parent_utility == "Pacific Gas & Electric Co":
                        caiso_rates = [
                            x
                            for x in caiso_rates
                            if x.caiso_report.query_params.get("node", None)
                            == "TH_NP15_GEN-APND"
                        ]
                    elif parent_utility == "Southern California Edison Co":
                        caiso_rates = [
                            x
                            for x in caiso_rates
                            if x.caiso_report.query_params.get("node", None)
                            == "TH_SP15_GEN-APND"
                        ]
                    single.caiso_rates.add(*caiso_rates)

                    single_scenario_study_ids.add(single.id)

        existing_multiple_scenario_studies = get_exact_many_to_many(
            model=MultipleScenarioStudy,
            m2m_field="single_scenario_studies",
            ids=single_scenario_study_ids,
        )
        if existing_multiple_scenario_studies:
            # return existing MultipleScenarioStudy
            multiple = existing_multiple_scenario_studies.first()
        else:
            # create new MultipleScenarioStudy
            multiple = MultipleScenarioStudy.objects.create(name=name)
            multiple.single_scenario_studies.add(
                *SingleScenarioStudy.objects.filter(
                    id__in=single_scenario_study_ids
                )
            )

        multiple.initialize()
        run_study.delay(multiple.id)

        return Response(
            StudySerializer(multiple, many=False).data,
            status=status.HTTP_201_CREATED,
        )


class StudyViewSet(ListRetrieveUpdateDestroyViewSet):
    """
    Study objects containing aggregate pre_der_intervalframe,
    der_intervalframe, and post_der_intervalframe data and report data.
    """

    model = Study
    serializer_class = StudySerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "data_types",
                required=False,
                location="query",
                description=(
                    "One or many data types to return. Choices are 'default', "
                    "'total', 'average', 'maximum', 'minimum', and 'count'."
                ),
            ),
            coreapi.Field(
                "column",
                required=False,
                location="query",
                description=(
                    "Column to run aggregate calculations on for data_types "
                    "other than default."
                ),
            ),
            coreapi.Field(
                "start",
                required=False,
                location="query",
                description=(
                    "Filter data to include only timestamps starting on or "
                    "after start. (Format: ISO 8601)"
                ),
            ),
            coreapi.Field(
                "end_limit",
                required=False,
                location="query",
                description=(
                    "Filter data to include only timestamps starting before "
                    "end_limit. (Format: ISO 8601)"
                ),
            ),
            coreapi.Field(
                "period",
                required=False,
                location="query",
                description="Integer representing the number of minutes in the dataframe period",
            ),
            coreapi.Field(
                "include[]",
                required=False,
                location="query",
                description=(
                    "deferred_fields disabled by default: ders, "
                    "der_simulations, meters, meter_groups, report, "
                    "report_summary."
                ),
            ),
        ]
    )

    def get_queryset(self):
        """
        Return only Study objects associated with authenticated user.
        """
        user = self.request.user

        ids = set()
        object_type = self.request.query_params.get("object_type")

        if not object_type or object_type == "SingleScenarioStudy":
            ids = ids | set(
                SingleScenarioStudy.objects.filter(
                    meter_group__owners=user
                ).values_list("id", flat=True)
            )
        if not object_type or object_type == "MultipleScenarioStudy":
            ids = ids | set(
                MultipleScenarioStudy.objects.filter(
                    single_scenario_studies__meter_group__owners=user
                ).values_list("id", flat=True)
            )

        return Study.objects.filter(id__in=ids)

    @action(methods=("get",), detail=False)
    def download(self, request, *args, **kwargs):
        """
        Generates a CSV file from a set of scenarios. The file contents can be
        configured to return customer-level data or scenario-level data via the
        `level` query parameter
        """
        # validate `ids`
        ids_param = self._param("ids")
        if not ids_param:
            raise serializers.ValidationError(
                "`ids` query parameter is required"
            )
        ids = ids_param.split(",")

        # validate `level`
        level = self._param("level")
        if level == "summary":
            export_key = "exportable_report_summary"
            download_kwargs = {"filename": "scenario_data"}
        elif level == "customer":
            export_key = "exportable_report"
            download_kwargs = {
                "filename": "customer_data",
                "index": False,
                "exclude": ["SingleScenarioStudy"],
            }
        else:
            raise serializers.ValidationError(
                "`level` query parameter is missing or unrecognized"
            )

        # filter for scenarios with the given IDs, with the caveat that the user
        # must be an owner (or they're not authorized to see them)
        scenarios = SingleScenarioStudy.objects.filter(
            id__in=ids, meter_group__owners=request.user
        )

        # iterate through the scenarios and build their contributions to the
        # CSV file
        dataframe = reduce(
            lambda df, scenario: df.append(
                getattr(scenario, export_key), sort=False
            ),
            scenarios,
            pd.DataFrame(),
        )

        return download_dataframe(dataframe, **download_kwargs)


class GHGRateViewSet(ListRetrieveViewSet):
    """
    GHGRate objects
    """

    model = GHGRate
    serializer_class = GHGRateSerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "include[]",
                required=False,
                location="query",
                description="deferred_fields disabled by default: data",
            ),
            coreapi.Field(
                "data_format",
                required=False,
                location="query",
                description=(
                    "Format of the data in the response. Choices are '288' "
                    "and 'interval'. This field is required if `data` is "
                    "included."
                ),
            ),
            coreapi.Field(
                "period",
                required=False,
                location="query",
                description=(
                    "Period of the interval data when `format` field is set "
                    "to `interval`. Choices are '1H' and '15M'. This has no "
                    "impact if the `format` is not `interval`. (Format: ISO "
                    "8601)"
                ),
            ),
            coreapi.Field(
                "start",
                required=False,
                location="query",
                description=(
                    "Beginning of the interval data when `format` field is "
                    "set to `interval`. This has no impact if the `format` is "
                    "not `interval`. (Format: ISO 8601)"
                ),
            ),
            coreapi.Field(
                "end_limit",
                required=False,
                location="query",
                description=(
                    "End limit of the interval data when `format` field is "
                    "set to `interval`. This has no impact if the `format` is "
                    "not `interval`. (Format: ISO 8601)"
                ),
            ),
        ]
    )


class CAISORateViewSet(ListRetrieveViewSet):
    """
    CAISORate objects
    """

    model = CAISORate
    serializer_class = CAISORateSerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "data_types",
                required=False,
                location="query",
                description=(
                    "One or many data types to return. Choices are 'default', "
                    "'total', 'average', 'maximum', 'minimum', and 'count'."
                ),
            ),
            coreapi.Field(
                "column",
                required=False,
                location="query",
                description=(
                    "Column to run aggregate calculations on for data_types "
                    "other than default."
                ),
            ),
            coreapi.Field(
                "start",
                required=False,
                location="query",
                description=(
                    "Filter data to include only timestamps starting on or "
                    "after start. (Format: ISO 8601)"
                ),
            ),
            coreapi.Field(
                "end_limit",
                required=False,
                location="query",
                description=(
                    "Filter data to include only timestamps starting before "
                    "end_limit. (Format: ISO 8601)"
                ),
            ),
            coreapi.Field(
                "period",
                required=False,
                location="query",
                description="Integer representing the number of minutes in the dataframe period",
            ),
        ]
    )
