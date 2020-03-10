import coreapi
import json
import pandas as pd
from rest_framework import serializers, status
from rest_framework.schemas import AutoSchema
from rest_framework.response import Response

from django.db import transaction

from beo_datastore.libs.api.serializers import require_request_data
from beo_datastore.libs.api.viewsets import (
    CreateViewSet,
    ListRetrieveDestroyViewSet,
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
        ]
    )

    def create(self, request):
        require_request_data(request, ["name", "meter_group_ids", "ders"])

        try:
            name = request.data["name"]
            meter_group_ids = json.loads(request.data["meter_group_ids"])
            ders = json.loads(request.data["ders"])
        except json.JSONDecodeError:
            raise serializers.ValidationError(
                "Cannot parse JSON input fields."
            )

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
                        rate_plan=RatePlan.objects.first(),  # TODO: fix this!
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


class StudyViewSet(ListRetrieveDestroyViewSet):
    """
    Study objects containing aggregate pre_der_intervalframe,
    der_intervalframe, and post_der_intervalframe data and report data.
    """

    queryset = Study.objects.all()
    serializer_class = StudySerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "ids",
                required=False,
                location="query",
                description=(
                    "True to return DERSimulation, Meter, and MeterGroup ids. "
                    "Defaults to false."
                ),
            ),
            coreapi.Field(
                "report",
                required=False,
                location="query",
                description=(
                    "True to return Study report. Defaults to false."
                ),
            ),
            coreapi.Field(
                "metadata",
                required=False,
                location="query",
                description=("False to remove metadata. Defaults to true."),
            ),
        ]
    )
