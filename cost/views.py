import coreapi
import pandas as pd
from rest_framework import serializers, status
from rest_framework.schemas import AutoSchema
from rest_framework.response import Response

from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction

from beo_datastore.libs.api.serializers import require_request_data
from beo_datastore.libs.api.viewsets import (
    CreateViewSet,
    ListRetrieveUpdateDestroyViewSet,
)
from beo_datastore.libs.models import get_exact_many_to_many

from cost.ghg.models import GHGRate
from cost.study.models import SingleScenarioStudy, MultipleScenarioStudy
from cost.utility_rate.models import RatePlan
from reference.reference_model.models import (
    DERConfiguration,
    DERStrategy,
    MeterGroup,
    Study,
)

from .serializers import MultipleScenarioStudySerializer, StudySerializer
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
                        try:
                            rate_plan = meter_group.primary_linked_rate_plan
                        except ObjectDoesNotExist:
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
                "include[]",
                required=False,
                location="query",
                description=(
                    "deferred_fields disabled by default: ders, "
                    "der_simulations, meters, meter_groups, report, "
                    "report_summary."
                ),
            )
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
