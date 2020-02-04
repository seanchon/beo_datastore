import coreapi
from rest_framework import status
from rest_framework.exceptions import UnsupportedMediaType
from rest_framework.response import Response
from rest_framework.schemas import AutoSchema

from beo_datastore.libs.api.viewsets import ListRetrieveDestroyViewSet

# from load.tasks import ingest_meters_from_file
from reference.reference_model.models import (
    LoadServingEntity,
    OriginFile,
    MeterIntervalFrame,
)

from .serializers import MeterIntervalFrameSerializer, OriginFileSerializer


class OriginFileViewSet(ListRetrieveDestroyViewSet):
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
            # ingest_meters_from_file.delay(origin_file.id)
            # TODO: return link to asset in response
            return Response(status=status.HTTP_204_NO_CONTENT)
        else:
            raise UnsupportedMediaType("Upload must be a .csv file.")


class MeterViewSet(ListRetrieveDestroyViewSet):
    """
    Meters and/or ReferenceBuildings with associated interval data.
    """

    queryset = MeterIntervalFrame.objects.all()
    serializer_class = MeterIntervalFrameSerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "data_types",
                required=False,
                location="query",
                description=(
                    "Data type or types to return. Choices are 'default', "
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
