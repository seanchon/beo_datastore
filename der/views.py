import coreapi
from rest_framework.schemas import AutoSchema

from beo_datastore.libs.api.viewsets import ListRetrieveViewSet

from der.simulation.models import DERConfiguration, DERStrategy, DERSimulation

from .serializers import (
    DERConfigurationSerializer,
    DERSimulationSerializer,
    DERStrategySerializer,
)


class DERConfigurationViewSet(ListRetrieveViewSet):
    """
    DER configurations used in DER simulations.
    """

    queryset = DERConfiguration.objects.all()
    serializer_class = DERConfigurationSerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "metadata",
                required=False,
                location="query",
                description=("False to remove metadata. Defaults to true."),
            )
        ]
    )


class DERSimulationViewSet(ListRetrieveViewSet):
    """
    DER simulations.
    """

    queryset = DERSimulation.objects.all()
    serializer_class = DERSimulationSerializer

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
                "metadata",
                required=False,
                location="query",
                description=("False to remove metadata. Defaults to true."),
            ),
        ]
    )


class DERStrategyViewSet(ListRetrieveViewSet):
    """
    DER strategies used in DER simulations.
    """

    queryset = DERStrategy.objects.all()
    serializer_class = DERStrategySerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "metadata",
                required=False,
                location="query",
                description=("False to remove metadata. Defaults to true."),
            )
        ]
    )
