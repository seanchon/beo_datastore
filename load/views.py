import coreapi
from rest_framework import status
from rest_framework.exceptions import UnsupportedMediaType
from rest_framework.response import Response
from rest_framework.schemas import AutoSchema

from beo_datastore.libs.api.viewsets import (
    CreateViewSet,
    ListRetrieveViewSet,
    ListRetrieveDestroyViewSet,
)

from load.tasks import (
    ingest_meters_from_file,
    aggregate_meter_group_intervalframes,
)
from load.customer.models import OriginFile
from reference.reference_model.models import (
    LoadServingEntity,
    Meter,
    MeterGroup,
)

from .serializers import (
    MeterSerializer,
    MeterGroupSerializer,
    OriginFileSerializer,
)


class OriginFileViewSet(CreateViewSet):
    """
    Files containing customer Meter data.
    """

    queryset = OriginFile.objects.all()
    serializer_class = OriginFileSerializer

    def create(self, request):
        file = request.data["file"]
        # TODO: add additional file validation
        if file.content_type == "text/csv":
            origin_file, _ = OriginFile.get_or_create(
                file=file,
                load_serving_entity=LoadServingEntity.objects.get(
                    id=request.data["load_serving_entity_id"]
                ),
                owner=request.user,
            )
            # TODO: ingest meters on EC2 instance
            ingest_meters_from_file(origin_file.id)
            aggregate_meter_group_intervalframes.delay(origin_file.id)
            # TODO: return link to asset in response
            return Response(status=status.HTTP_204_NO_CONTENT)
        else:
            raise UnsupportedMediaType("Upload must be a .csv file.")


class MeterGroupViewSet(ListRetrieveDestroyViewSet):
    """
    OriginFiles, CustomerPopulations, and/or CustomerClusters with associated
    aggregated interval data.
    """

    queryset = MeterGroup.objects.all()
    serializer_class = MeterGroupSerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "meters",
                required=False,
                location="query",
                description=(
                    "True to return meter ids. No ids returned by default."
                ),
            ),
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
        ]
    )


class MeterViewSet(ListRetrieveViewSet):
    """
    CustomerMeters and/or ReferenceMeters with associated interval data.
    """

    queryset = Meter.objects.all()
    serializer_class = MeterSerializer
    filterset_fields = ("meter_groups",)

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "meter_groups",
                required=False,
                location="query",
                description="A single meter_group id.",
            ),
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
        ]
    )
