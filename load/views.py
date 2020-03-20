import coreapi
from rest_framework import serializers, status
from rest_framework.exceptions import UnsupportedMediaType
from rest_framework.response import Response
from rest_framework.schemas import AutoSchema

from beo_datastore.libs.api.serializers import require_request_data
from beo_datastore.libs.api.viewsets import (
    CreateViewSet,
    ListRetrieveViewSet,
    ListRetrieveDestroyViewSet,
)

from load.tasks import ingest_origin_file_meters
from load.customer.models import OriginFile
from reference.reference_model.models import Meter, MeterGroup
from reference.auth_user.models import LoadServingEntity

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

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "file",
                required=True,
                location="body",
                description=("File containing Meter data."),
            ),
            coreapi.Field(
                "name",
                required=True,
                location="body",
                description=("File name."),
            ),
            coreapi.Field(
                "load_serving_entity_id",
                required=True,
                location="body",
                description=("LoadServingEntity ID."),
            ),
        ]
    )

    def create(self, request):
        require_request_data(request, ["file", "name"])

        file = request.data["file"]
        name = request.data["name"]
        if request.user.profile.load_serving_entity:
            load_serving_entity = request.user.profile.load_serving_entity
        elif request.user.is_staff or request.user.is_superuser:
            require_request_data(request, ["load_serving_entity_id"])
            load_serving_entity = LoadServingEntity.objects.get(
                id=request.data["load_serving_entity_id"]
            )
        else:
            raise serializers.ValidationError(
                "User must be associated with LoadServingEntity or User must "
                "staff or superuser and LoadServingEntity id must be provided."
            )
        # TODO: add additional file validation

        if file.content_type == "text/csv":
            origin_file, _ = OriginFile.get_or_create(
                file=file,
                name=name,
                load_serving_entity=load_serving_entity,
                owner=request.user,
            )
            ingest_origin_file_meters.delay(origin_file.id, overwrite=True)
            return Response(
                MeterGroupSerializer(origin_file, many=False).data,
                status=status.HTTP_201_CREATED,
            )
        else:
            raise UnsupportedMediaType("Upload must be a .csv file.")


class MeterGroupViewSet(ListRetrieveDestroyViewSet):
    """
    OriginFiles, CustomerPopulations, and/or CustomerClusters with associated
    aggregated interval data.
    """

    model = MeterGroup
    serializer_class = MeterGroupSerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "ids",
                required=False,
                location="query",
                description=("True to return Meter ids. Defaults to false."),
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
            coreapi.Field(
                "metadata",
                required=False,
                location="query",
                description=("False to remove metadata. Defaults to true."),
            ),
        ]
    )

    def get_queryset(self):
        """
        Return only MeterGroup objects associated with authenticated user.
        """
        user = self.request.user
        return MeterGroup.objects.filter(owners=user)


class MeterViewSet(ListRetrieveViewSet):
    """
    CustomerMeters and/or ReferenceMeters with associated interval data.
    """

    model = Meter
    serializer_class = MeterSerializer
    filterset_fields = ("meter_groups",)

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "meter_groups",
                required=False,
                location="query",
                description="Filter meters by a single meter_group id.",
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
            coreapi.Field(
                "metadata",
                required=False,
                location="query",
                description=("False to remove metadata. Defaults to true."),
            ),
        ]
    )

    def get_queryset(self):
        """
        Return only Meter objects associated with authenticated user.
        """
        user = self.request.user
        return Meter.objects.filter(meter_groups__owners=user)
