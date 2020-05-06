from distutils.util import strtobool

import coreapi
from rest_framework import serializers, status
from rest_framework.response import Response
from rest_framework.schemas import AutoSchema

from django.core.exceptions import ValidationError

from beo_datastore.libs.api.serializers import require_request_data
from beo_datastore.libs.api.viewsets import (
    CreateViewSet,
    ListRetrieveViewSet,
    ListRetrieveDestroyViewSet,
)
from beo_datastore.libs.models import get_model_from_any_app

from load.tasks import create_clusters, ingest_origin_file_meters
from load.customer.models import (
    CustomerCluster,
    CustomerPopulation,
    OriginFile,
)
from reference.reference_model.models import Meter, MeterGroup
from reference.auth_user.models import LoadServingEntity

from .serializers import (
    CustomerClusterSerializer,
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
                required=False,
                location="body",
                description=(
                    "LoadServingEntity ID if user not associated with an LSE."
                ),
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


class CustomerClusterViewSet(CreateViewSet):
    """
    K-means clustered MeterGroup.
    """

    queryset = CustomerCluster.objects.all()
    serializer_class = CustomerClusterSerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "meter_group_id",
                required=True,
                location="body",
                description=("MeterGroup id."),
            ),
            coreapi.Field(
                "type",
                required=True,
                location="body",
                description=("Choices: average, maximum, minimum, total"),
            ),
            coreapi.Field(
                "number_of_clusters",
                required=True,
                location="body",
                description=("Number of k-means clusters to create."),
            ),
            coreapi.Field(
                "normalize",
                required=True,
                location="body",
                description=("Normalize meter data: true or false."),
            ),
        ]
    )

    def create(self, request):
        require_request_data(
            request,
            ["meter_group_id", "type", "number_of_clusters", "normalize"],
        )

        try:
            meter_group = MeterGroup.objects.get(
                id=request.data["meter_group_id"]
            )
        except MeterGroup.DoesNotExist():
            raise serializers.ValidationError("MeterGroup does not exist.")

        try:
            customer_population, _ = CustomerPopulation.objects.get_or_create(
                name=meter_group.name,
                frame288_type=request.data["type"] + "_frame288",
                number_of_clusters=request.data["number_of_clusters"],
                normalize=strtobool(request.data["normalize"]),
                meter_group=meter_group,
            )
        except (ValueError, ValidationError) as e:
            raise serializers.ValidationError(e)
        create_clusters.delay(
            customer_population_id=customer_population.id,
            owner_id=request.user.id,
        )

        return Response(status=status.HTTP_204_NO_CONTENT)


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
                "include[]",
                required=False,
                location="query",
                description=("deferred_fields disabled by default: meters."),
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
                "object_type",
                required=False,
                location="query",
                description=("Filter by object_type field."),
            ),
        ]
    )

    def get_queryset(self):
        """
        Return only MeterGroup objects associated with authenticated user.
        """
        user = self.request.user

        object_type = self.request.query_params.get("object_type")

        if object_type:
            model = get_model_from_any_app(object_type, MeterGroup)
            if not model:
                raise serializers.ValidationError("Invalid object_type.")
        else:
            model = MeterGroup

        return model.objects.filter(owners=user)


class MeterViewSet(ListRetrieveViewSet):
    """
    CustomerMeters and/or ReferenceMeters with associated interval data.
    """

    model = Meter
    serializer_class = MeterSerializer

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
                "object_type",
                required=False,
                location="query",
                description=("Filter by object_type field."),
            ),
        ]
    )

    def get_queryset(self):
        """
        Return only Meter objects associated with authenticated user.
        """
        user = self.request.user

        object_type = self.request.query_params.get("object_type")

        if object_type:
            model = get_model_from_any_app(object_type, Meter)
            if not model:
                raise serializers.ValidationError("Invalid object_type.")
        else:
            model = Meter

        return model.objects.filter(meter_groups__owners=user)
