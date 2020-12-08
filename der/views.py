import coreapi
from django.core.exceptions import ValidationError
from django.db.models import Q
from rest_framework import serializers, status
from rest_framework.response import Response
from rest_framework.schemas import AutoSchema

from beo_datastore.libs.api.viewsets import ListRetrieveViewSet
from cost.ghg.models import GHGRate
from cost.procurement.models import SystemProfile
from cost.utility_rate.models import RatePlan
from der.simulation.scripts.generate_der_strategy import (
    generate_ra_reduction_battery_strategy,
    generate_bill_reduction_battery_strategy,
    generate_ghg_reduction_battery_strategy,
    generate_commuter_evse_strategy,
)

from der.simulation.models import (
    DERConfiguration,
    DERStrategy,
    DERSimulation,
    BatteryConfiguration,
    BatteryStrategy,
    EVSEConfiguration,
    EVSEStrategy,
    SolarPVConfiguration,
    SolarPVStrategy,
)

from .serializers import (
    DERConfigurationSerializer,
    DERSimulationSerializer,
    DERStrategySerializer,
)


class DERConfigurationViewSet(ListRetrieveViewSet):
    """
    DER configurations used in DER simulations.
    """

    model = DERConfiguration
    serializer_class = DERConfigurationSerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "include[]",
                required=False,
                location="query",
                description=("deferred_fields disabled by default: data. "),
            )
        ]
    )

    def get_queryset(self, queryset=None):
        """
        Enables filtering by DER type
        """
        der_type = self._param("der_type")

        if der_type == "Battery":
            model = BatteryConfiguration
        elif der_type == "EVSE":
            model = EVSEConfiguration
        elif der_type == "SolarPV":
            model = SolarPVConfiguration
        else:
            model = DERConfiguration

        # Only filter for configurations within the user's LSE. A configuration
        # missing an LSE is visible to everyone.
        lse = self.request.user.profile.load_serving_entity
        return model.objects.filter(
            Q(load_serving_entity__isnull=True) | Q(load_serving_entity=lse)
        )

    def create(self, request):
        self._require_data_fields("der_type")
        [der_type] = self._data(["der_type"])

        try:
            if der_type == "Battery":
                configuration, created = self.create_battery_configuration(
                    request
                )
            elif der_type == "EVSE":
                configuration, created = self.create_evse_configuration(request)
            elif der_type == "SolarPV":
                configuration, created = self.create_solar_configuration(
                    request
                )
            else:
                raise serializers.ValidationError(
                    f"der_type parameter has unrecognized type: {der_type}"
                )
        except ValidationError as e:
            raise serializers.ValidationError(detail=e.message_dict)

        if not created:
            raise serializers.ValidationError(
                "BatteryConfiguration with provided parameters already exists!"
            )

        return Response(
            DERConfigurationSerializer(configuration, many=False).data,
            status=status.HTTP_201_CREATED,
        )

    def create_battery_configuration(self, request):
        """
        Creates a BatteryConfiguration
        """
        configuration_attrs = [
            "discharge_duration_hours",
            "efficiency",
            "name",
            "rating",
        ]

        self._require_data_fields(*configuration_attrs)
        discharge_duration_hours, efficiency, name, rating = self._data(
            configuration_attrs
        )

        return BatteryConfiguration.objects.get_or_create(
            discharge_duration_hours=discharge_duration_hours,
            efficiency=efficiency,
            load_serving_entity=request.user.profile.load_serving_entity,
            name=name,
            rating=rating,
        )

    def create_evse_configuration(self, request):
        """
        Creates a EVSEConfiguration
        """
        configuration_attrs = [
            "ev_capacity",
            "ev_count",
            "ev_efficiency",
            "ev_mpkwh",
            "evse_count",
            "evse_rating",
            "evse_utilization",
            "name",
        ]

        self._require_data_fields(*configuration_attrs)
        (
            ev_capacity,
            ev_count,
            ev_efficiency,
            ev_mpkwh,
            evse_count,
            evse_rating,
            evse_utilization,
            name,
        ) = self._data(configuration_attrs)

        return EVSEConfiguration.objects.get_or_create(
            name=name,
            ev_capacity=ev_capacity,
            ev_count=ev_count,
            ev_efficiency=ev_efficiency,
            ev_mpkwh=ev_mpkwh,
            evse_count=evse_count,
            evse_rating=evse_rating,
            evse_utilization=evse_utilization,
            load_serving_entity=request.user.profile.load_serving_entity,
        )

    def create_solar_configuration(self, request):
        """
        Creates a SolarConfiguration
        """
        configuration_attrs = [
            "address",
            "array_type",
            "azimuth",
            "name",
            "tilt",
        ]

        self._require_data_fields(*configuration_attrs)
        (address, array_type, azimuth, name, tilt) = self._data(
            configuration_attrs
        )

        return SolarPVConfiguration.get_or_create_from_attrs(
            address=address,
            array_type=array_type,
            azimuth=azimuth,
            load_serving_entity=request.user.profile.load_serving_entity,
            name=name,
            tilt=tilt,
        )


class DERSimulationViewSet(ListRetrieveViewSet):
    """
    DER simulations.
    """

    model = DERSimulation
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
                "period",
                required=False,
                location="query",
                description="Integer representing the number of minutes in the dataframe period",
            ),
        ]
    )

    def get_queryset(self):
        """
        Return only DERSimulation objects associated with authenticated user.
        """
        user = self.request.user
        return DERSimulation.objects.filter(meter__meter_groups__owners=user)


class DERStrategyViewSet(ListRetrieveViewSet):
    """
    DER strategies used in DER simulations.
    """

    model = DERStrategy
    serializer_class = DERStrategySerializer

    schema = AutoSchema(
        manual_fields=[
            coreapi.Field(
                "include[]",
                required=False,
                location="query",
                description=("deferred_fields disabled by default: data. "),
            )
        ]
    )

    def get_queryset(self, queryset=None):
        """
        Enables filtering by DER type
        """
        der_type = self._param("der_type")

        if der_type == "Battery":
            model = BatteryStrategy
        elif der_type == "EVSE":
            model = EVSEStrategy
        elif der_type == "SolarPV":
            model = SolarPVStrategy
        else:
            model = DERStrategy

        # Only filter for strategies within the user's LSE. A strategy missing
        # an LSE is visible to everyone.
        lse = self.request.user.profile.load_serving_entity
        return model.objects.filter(
            Q(load_serving_entity__isnull=True) | Q(load_serving_entity=lse)
        )

    def create(self, request):
        self._require_data_fields("name", "der_type")
        [der_type] = self._data(["der_type"])

        try:
            if der_type == "Battery":
                strategy = self.create_battery_strategy(request)
            elif der_type == "EVSE":
                strategy = self.create_evse_strategy(request)
            elif der_type == "SolarPV":
                strategy = self.create_solar_strategy(request)
            else:
                raise serializers.ValidationError(
                    detail=f"der_type parameter has unrecognized value: {der_type}"
                )
        except ValidationError as e:
            raise serializers.ValidationError(detail=e.message_dict)

        return Response(
            DERStrategySerializer(strategy, many=False).data,
            status=status.HTTP_201_CREATED,
        )

    def create_battery_strategy(self, request):
        """
        Creates a BatteryStrategy
        """
        strategy_attrs = [
            "name",
            "charge_from_grid",
            "discharge_to_grid",
            "cost_function",
        ]

        self._require_data_fields(*strategy_attrs)
        name, charge_grid, discharge_grid, cost_fn, description = self._data(
            strategy_attrs + ["description"]
        )

        strategy_generation_args = {
            "charge_grid": charge_grid,
            "description": description,
            "discharge_grid": discharge_grid,
            "load_serving_entity": request.user.profile.load_serving_entity,
            "name": name,
        }

        # The cost function should come with an `object_type` and an `id` field
        cost_fn_type = cost_fn.get("object_type")
        cost_fn_id = cost_fn.get("id")

        if cost_fn_type == "RatePlan":
            return generate_bill_reduction_battery_strategy(
                **strategy_generation_args,
                rate_plan=RatePlan.objects.get(id=cost_fn_id),
            )
        elif cost_fn_type == "SystemProfile":
            return generate_ra_reduction_battery_strategy(
                **strategy_generation_args,
                system_profile=SystemProfile.objects.get(id=cost_fn_id),
            )
        elif cost_fn_type == "GHGRate":
            return generate_ghg_reduction_battery_strategy(
                **strategy_generation_args,
                ghg_rate=GHGRate.objects.get(id=cost_fn_id),
            )
        else:
            raise serializers.ValidationError(
                f"cost_function parameter has unrecognized type: {cost_fn_type}"
            )

    def create_evse_strategy(self, request):
        """
        Creates a EVSEStrategy
        """
        strategy_attrs = [
            "charge_off_nem",
            "distance",
            "end_charge_hour",
            "start_charge_hour",
            "name",
        ]

        self._require_data_fields(*strategy_attrs)
        (
            charge_off_nem,
            distance,
            end_charge_hour,
            start_charge_hour,
            name,
            description,
        ) = self._data(strategy_attrs + ["description"])

        return generate_commuter_evse_strategy(
            charge_off_nem=charge_off_nem,
            distance=distance,
            end_charge_hour=end_charge_hour,
            start_charge_hour=start_charge_hour,
            load_serving_entity=request.user.profile.load_serving_entity,
            name=name,
            user_description=description,
        )

    def create_solar_strategy(self, request):
        """
        Creates a SolarStrategy
        """
        strategy_attrs = ["name", "serviceable_load_ratio"]
        self._require_data_fields(*strategy_attrs)
        name, serviceable_load_ratio, description = self._data(
            strategy_attrs + ["description"]
        )

        solar_strategy, created = SolarPVStrategy.objects.get_or_create(
            description=description,
            name=name,
            load_serving_entity=request.user.profile.load_serving_entity,
            parameters={"serviceable_load_ratio": serviceable_load_ratio},
        )

        if not created:
            raise serializers.ValidationError(
                "SolarConfiguration with provided parameters already exists!"
            )

        return solar_strategy
