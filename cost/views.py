import json
from datetime import datetime
from functools import reduce

import coreapi
import numpy as np
import pandas as pd
from django.db import transaction
from django.db.models import Q, deletion
from django.http.request import QueryDict
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.schemas import AutoSchema

from beo_datastore.libs.api.viewsets import (
    CreateListRetrieveDestroyViewSet,
    CreateListRetrieveUpdateDestroyViewSet,
    ListRetrieveViewSet,
)
from beo_datastore.libs.dataframe import download_dataframe
from cost.utility_rate.models import RatePlan
from cost.procurement.models import CAISORate, SystemProfile
from cost.study.models import Scenario
from cost.utility_rate.libs import (
    convert_rate_df_to_dict,
    convert_rate_dict_to_df,
)
from navigader_core.load.dataframe import get_dataframe_period
from reference.auth_user.models import LoadServingEntity
from reference.reference_model.models import (
    DERConfiguration,
    DERStrategy,
    MeterGroup,
)
from .serializers import (
    CAISORateSerializer,
    GHGRateSerializer,
    RateCollectionSerializer,
    RatePlanSerializer,
    ScenarioSerializer,
    SystemProfileSerializer,
)
from .tasks import run_scenario

# Constants for time in seconds
QUARTER_HOUR = 900
HOUR = 3600


class ScenarioViewSet(CreateListRetrieveUpdateDestroyViewSet):
    """
    Scenario objects containing aggregate pre_der_intervalframe,
    der_intervalframe, and post_der_intervalframe data and report data.
    """

    serializer_class = ScenarioSerializer

    schema = AutoSchema(
        manual_fields=[
            # POST fields
            coreapi.Field(
                "name",
                required=False,
                location="body",
                description="Scenario name.",
            ),
            coreapi.Field(
                "meter_group_ids",
                required=False,
                location="body",
                description=(
                    "JSON List of MeterGroup ids. "
                    "Ex. ['<meter_group_id>', '<meter_group_id>']"
                ),
            ),
            coreapi.Field(
                "ders",
                required=False,
                location="body",
                description=(
                    "JSON List of der_configuration_id, der_strategy_id pairs. "
                    "Ex. [{'der_configuration_id': '<id>', "
                    "'der_strategy_id': '<id>'}, ]"
                ),
            ),
            coreapi.Field(
                "cost_functions",
                required=False,
                location="body",
                description=(
                    "dictionary mapping a cost function-type to the ID of the "
                    "cost function to apply. Expected keys are 'rate_plan', "
                    "'ghg_rate', 'procurement_rate' and 'system_profile'. No "
                    "key is required. The 'rate_plan' key can be set to 'auto' "
                    "for automatic rate plan assignment."
                ),
            ),
            coreapi.Field(
                "object_type",
                required=False,
                location="query",
                description=("Filter by object_type field."),
            ),
            # GET fields
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

    def create(self, request):
        self._require_data_fields("name", "meter_group_ids", "ders")

        lse = request.user.profile.load_serving_entity
        name, meter_group_ids, scenario_ids, ders, cost_functions = self._data(
            [
                "name",
                "meter_group_ids",
                "scenario_ids",
                "ders",
                "cost_functions",
            ]
        )

        cost_function_map = {
            "rate_plan": RatePlan,
            "procurement_rate": CAISORate,
            "system_profile": SystemProfile,
        }

        year = None
        for meter_group_id in meter_group_ids:
            mg = MeterGroup.objects.get(id=meter_group_id)
            if len(mg.years) != 1:
                raise serializers.ValidationError(
                    "Meter group spans multiple years"
                )

            mg_year = mg.years[0]
            if year is None:
                year = mg_year
            elif year != mg_year:
                raise serializers.ValidationError(
                    "Encountered two meter groups without shared year"
                )

        for cost_function_name, cost_function_id in cost_functions.items():
            if cost_function_id == "auto" or cost_function_name == "ghg_rate":
                continue
            cost_function = cost_function_map[cost_function_name].objects.get(
                id=cost_function_id
            )
            if cost_function_name == "rate_plan":
                if cost_function.start_date.year > year:
                    raise serializers.ValidationError(
                        "Rate Plan starts later than interval data"
                    )
            else:
                if cost_function.intervalframe.dataframe.index[0].year != year:
                    raise serializers.ValidationError(
                        "Encountered a cost function with the wrong year"
                    )

        with transaction.atomic():
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

                    scenario, _ = Scenario.objects.get_or_create(
                        start=pd.Timestamp.min,
                        # Bug: Django rounds pd.Timestamp.max up on save
                        end_limit=pd.Timestamp.max.replace(microsecond=0),
                        der_configuration=der_configuration,
                        der_strategy=der_strategy,
                        load_serving_entity=lse,
                        meter_group=meter_group,
                        name=name,
                    )

                    scenario.owners.add(*meter_group.owners.all())
                    scenario.assign_cost_functions(cost_functions)
                    run_scenario.delay(scenario.id)

        return Response(
            ScenarioSerializer(scenario, many=False).data,
            status=status.HTTP_201_CREATED,
        )

    def get_queryset(self):
        """
        Return only Scenario objects associated with authenticated user.
        """
        user = self.request.user
        return Scenario.objects.filter(meter_group__owners=user)

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
                "exclude": ["ScenarioID"],
            }
        else:
            raise serializers.ValidationError(
                "`level` query parameter is missing or unrecognized"
            )

        # filter for scenarios with the given IDs, with the caveat that the user
        # must be an owner (or they're not authorized to see them)
        scenarios = Scenario.objects.filter(
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


class CostFunctionViewSet(CreateListRetrieveDestroyViewSet):
    """
    Provides common `get_queryset` and `destroy` methods for the cost functions.
    Users are permitted to access cost functions if the cost function belongs to
    the user's LSE or if it isn't associated with any LSE. Users are permitted
    to delete cost functions if the cost function belongs to the user's LSE.
    """

    def get_queryset(self, queryset=None):
        lse = self.request.user.profile.load_serving_entity
        return self.get_serializer().Meta.model.objects.filter(
            Q(load_serving_entity__isnull=True) | Q(load_serving_entity=lse)
        )

    def get_cost_fn_lse(self) -> LoadServingEntity:
        """
        Returns the LSE associated with the cost function object. For almost all
        cost function classes the relationship with the LSE is managed by the
        serializer model itself, but for `RateCollection` it is managed by the
        parent `RatePlan` object.
        """
        return self.get_object().load_serving_entity

    def user_can_delete_cost_fn(self, request):
        """
        Returns True if the user has permission to delete the provided cost
        function object. Cost function deletion permissioning is handled at the
        LSE level: if a user is a member of the same LSE as the cost function,
        they are permitted to delete it. If the cost function is not associated
        with any LSE, no user is permitted to delete it.

        :param request: The Django request object
        """
        lse = self.get_cost_fn_lse()
        user_lse = request.user.profile.load_serving_entity

        # Disallow deletion if the LSE doesn't precisely match
        has_lse = lse is not None
        same_lse = lse == user_lse
        return has_lse and same_lse

    def destroy(self, request, *args, **kwargs):
        if self.user_can_delete_cost_fn(request):
            try:
                return super().destroy(request, *args, **kwargs)
            except deletion.ProtectedError:
                raise serializers.ValidationError(
                    "This object cannot be deleted because it is being used."
                )
        else:
            self.permission_denied(request)


class CAISORateViewSet(CostFunctionViewSet):
    serializer_class = CAISORateSerializer

    class CustomSchema(AutoSchema):
        manual_fields = []

        def get_manual_fields(self, path: str, method: str):
            custom_fields = []
            if method.upper() == "GET":
                custom_fields = [
                    coreapi.Field(
                        "data_types",
                        required=False,
                        location="query",
                        description=(
                            "One or many data types to return. Choices are 'default', "
                            "'total', 'average', 'maximum', 'minimum', and 'count'."
                        ),
                    ),
                ]
            if method.upper() == "POST":
                custom_fields = [
                    coreapi.Field(
                        "file",
                        required=True,
                        location="body",
                        description=(
                            "CAISO rate 8760 or 4x8760 interval CSV. "
                            "1st column intervals in 1/1/19 1:00 format. "
                            "2nd $/Power or $/Energy rate value. "
                        ),
                    ),
                    coreapi.Field(
                        "name",
                        required=False,
                        location="body",
                        description=(
                            "Optional 'name' for ingested file instead of actual filename. "
                        ),
                    ),
                ]
            return self._manual_fields + custom_fields

    schema = CustomSchema()

    def create(self, request, *args, **kwargs):
        """
        Handle upload and ingestion of CAISO CSV interval file.
        """
        self._require_data_fields("file")
        [file, name] = self._data(["file", "name"])

        dataframe = self._read_interval_csv(file)
        load_serving_entity = request.user.profile.load_serving_entity
        name = name or file.name.split(".")[0]
        try:
            new_instance, created = CAISORate.get_or_create(
                dataframe=dataframe,
                load_serving_entity=load_serving_entity,
                name=name,
            )
        except serializers.ValidationError as e:
            raise serializers.ValidationError(detail=e.message_dict)

        if not created:
            model_class = self.get_serializer().Meta.model
            raise serializers.ValidationError(
                f"{model_class.__name__} with provided parameters already "
                f"exists!"
            )

        return Response(
            {
                "caiso_rate": self.serializer_class(
                    new_instance, many=False
                ).data
            },
            status=status.HTTP_201_CREATED,
        )

    @staticmethod
    def _read_interval_csv(file: str) -> pd.DataFrame:
        """
        Read, parse, and convert CAISO interval csv to interval dataframe.
        """
        try:
            df = pd.read_csv(file)
            # Clean redundant empty rows or columns if any.
            df.dropna(axis=0, how="all", inplace=True)
            df.dropna(axis=1, how="all", inplace=True)
        except Exception as e:
            raise serializers.ValidationError(
                "Could not convert uploaded interval csv file to DataFrame.", e
            )

        # Ensure intervals are consistent.
        timestamp_column = df.columns[0]
        indices = pd.DatetimeIndex(df[timestamp_column])
        intervals = set(np.diff(indices))
        if len(intervals) != 1:
            raise serializers.ValidationError(
                "Timestamp intervals are inconsistent. Found the following "
                f"intervals: {intervals}"
            )

        interval: int = get_dataframe_period(
            df, by_column=timestamp_column, n=None
        ).seconds

        if interval not in [QUARTER_HOUR, HOUR]:
            raise serializers.ValidationError(
                f"Expected intervals are either 15 or 60 minutes. "
                f"Found '{interval / 60}' minutes."
            )

        # Validate that upload file contains interval readings `up to` 366 days.
        first_interval: pd.Timestamp = indices[0]
        last_interval: pd.Timestamp = indices[-1]

        span_days = (last_interval - first_interval).days
        if span_days > 366:
            raise serializers.ValidationError(
                "Upload CSV intervals span exceeds 366 days. "
                f"There are {span_days} days between {first_interval} and {last_interval}."
            )

        # Rename intervals with `start` datetime instead of `end` datetime.
        indices = indices - pd.to_timedelta(interval, unit="second")

        try:
            df.set_index(
                keys=indices, verify_integrity=True, drop=True, inplace=True,
            )
        except Exception as e:
            raise serializers.ValidationError(
                "Duplicates, inconsistent or missing interval timestamps. ", e
            )
        df.drop(columns=timestamp_column, inplace=True)
        df.index.rename("start", inplace=True)

        # Convert dollar per (energy or power) readings from one of the expected
        # headers/units  kW, kWh, MW, MWH, GW, or GWH into `Power in kW`.
        value = df.columns[0]
        unit = value.upper()
        interval_per_hour = HOUR / interval
        if "KWH" in unit:
            df[value] *= interval_per_hour
        elif "KW" in unit:
            pass
        elif "MWH" in unit:
            df[value] *= interval_per_hour / 1e3
        elif "MW" in unit:
            df[value] /= 1e3
        elif "GWH" in unit:
            df[value] *= interval_per_hour / 1e6
        elif "GW" in unit:
            df[value] /= 1e6
        else:
            raise serializers.ValidationError(
                f"'{unit}' is not an expected unit for aggregated energy or power values. "
                f"Unit should be one of kW, kWh, MW, MWH, GW, or GWH."
            )
        df.rename(columns={value: "$/kwh"}, inplace=True)

        return df

    @action(methods=("get",), detail=True)
    def download(self, request, pk, *args, **kwargs):
        """
        Downloads the CSV file representation of the `CAISORate`
        """
        caiso_rate = self.get_queryset().get(id=pk)
        return download_dataframe(
            caiso_rate.intervalframe.dataframe,
            index=True,
            filename="procurement-rate-data.csv",
        )


class RatePlanViewSet(CostFunctionViewSet):
    """
    Utility Rate Plan Objects
    """

    serializer_class = RatePlanSerializer

    def create(self, request, *args, **kwargs):
        user = request.user
        lse = user.profile.load_serving_entity_id
        request.data["load_serving_entity"] = lse
        return super().create(request, *args, **kwargs)


class RateCollectionViewSet(CostFunctionViewSet):
    """
    Utility Rate Data for a particular effective date
    """

    serializer_class = RateCollectionSerializer

    def get_queryset(self, queryset=None):
        lse = self.request.user.profile.load_serving_entity
        return self.get_serializer().Meta.model.objects.filter(
            Q(rate_plan__load_serving_entity__isnull=True)
            | Q(rate_plan__load_serving_entity=lse)
        )

    def get_cost_fn_lse(self):
        """
        Returns the LSE associated with the rate collection's parent rate plan
        model
        """
        return self.get_object().rate_plan.load_serving_entity

    def create(self, request, **kwargs):
        """
        Checks for 'rate_data_csv' in the request body and converts it to json
        in order to use it as the 'rate_data' field. If possible, the
        'effective_date' and 'utility_url' fields are filled with data
        from the 'rate_data' dictionary itself.
        """
        rate_data_csv = request.data.get("rate_data_csv", None)
        if rate_data_csv is not None:
            df = pd.read_csv(rate_data_csv.file)
            try:
                out_dict = convert_rate_df_to_dict(df)
            except Exception as e:
                raise serializers.ValidationError(
                    f"{e.__class__.__name__}, {str(e)}"
                )
            request.data["rate_data"] = out_dict
        elif "rate_data" not in request.data:
            raise serializers.ValidationError(
                "'rate_data_csv' or 'rate_data' is required"
            )
        file_date = request.data["rate_data"].get("effectiveDate", None)
        param_date = request.data.get("effective_date", None)
        if file_date is not None:
            request.data["effective_date"] = (
                datetime.fromtimestamp(int(file_date["$date"] / 1000)).date()
                if param_date is None
                else param_date
            )
        utility_url = request.data.get("utility_url", None)
        request.data["utility_url"] = (
            request.data["rate_data"].get("sourceReference", None)
            if utility_url is None
            else utility_url
        )
        if isinstance(request.data, QueryDict):
            request.data["rate_data"] = json.dumps(request.data["rate_data"])
        return super().create(request, **kwargs)

    @action(methods=("get",), detail=True)
    def download(self, request, pk, *args, **kwargs):
        """
        Downloads the CSV file representation of the `RateCollection`
        """
        rate_collection = self.get_queryset().get(id=pk)
        rate_collection_date = rate_collection.effective_date.strftime("%Y%m%d")
        df = convert_rate_dict_to_df(rate_collection.rate_data)
        filename = f"rate_collection-{rate_collection_date}"
        return download_dataframe(df, index=False, filename=filename)


class SystemProfileViewSet(CostFunctionViewSet):
    serializer_class = SystemProfileSerializer

    class CustomProfileSchema(AutoSchema):
        manual_fields = []

        def get_manual_fields(self, path: str, method: str):
            custom_fields = []
            if method.upper() == "GET":
                custom_fields = [
                    coreapi.Field(
                        "data_types",
                        required=False,
                        location="query",
                        description=(
                            "One or many data types to return. Choices are 'default', "
                            "'total', 'average', 'maximum', 'minimum', and 'count'."
                        ),
                    ),
                ]
            if method.upper() == "POST":
                custom_fields = [
                    coreapi.Field(
                        "file",
                        required=True,
                        location="body",
                        description=(
                            "CSV file that contains a load serving entity system-profile intervals. "
                            "1st column header is arbitrary. "
                            "1st column is to be timestamps with consistent 15 or 60 minutes intervals. "
                            "2nd column header must be one of: kW, kWh, MW, MWh, GW, or GWh. "
                            "2nd column is to be numeric readings values for each timestamp."
                        ),
                    ),
                    coreapi.Field(
                        "name",
                        required=False,
                        location="body",
                        description=(
                            "System profile `name`. If not provided filename will be used instead."
                        ),
                    ),
                    coreapi.Field(
                        "resource_adequacy_rate",
                        required=True,
                        location="body",
                        description="$/kW value used in RA cost calculations",
                    ),
                ]
            return self._manual_fields + custom_fields

    schema = CustomProfileSchema()

    def create(self, request, *args, **kwargs):
        """
        Handle upload for a system profile annual interval in CSV
        and ingest as a new SystemProfile instance.
        """
        self._require_data_fields("file", "resource_adequacy_rate")

        [file, name, resource_adequacy_rate] = self._data(
            ["file", "name", "resource_adequacy_rate"]
        )

        load_serving_entity = request.user.profile.load_serving_entity
        dataframe = self._read_interval_csv(file)
        name = name or file.name.split(".")[0]
        try:
            system_profile, created = SystemProfile.get_or_create(
                dataframe=dataframe,
                name=name,
                load_serving_entity=load_serving_entity,
                resource_adequacy_rate=resource_adequacy_rate,
            )
        except serializers.ValidationError as e:
            raise serializers.ValidationError(detail=e.message_dict)

        if not created:
            raise serializers.ValidationError(
                "SystemProfile with provided parameters already exists!"
            )

        return Response(
            {
                "system_profile": self.serializer_class(
                    system_profile, many=False
                ).data
            },
            status=status.HTTP_201_CREATED,
        )

    @staticmethod
    def _read_interval_csv(file) -> pd.DataFrame:

        df = pd.read_csv(file)
        # Clean redundant empty rows or columns if any.
        df.dropna(axis=0, how="all", inplace=True)
        df.dropna(axis=1, how="all", inplace=True)

        # Ensure intervals are consistent.
        timestamp_column = df.columns[0]
        indices = pd.DatetimeIndex(df[timestamp_column])
        intervals = set(np.diff(indices))
        if len(intervals) != 1:
            raise serializers.ValidationError(
                "Timestamp intervals are inconsistent. Found the following "
                f"intervals: {intervals}"
            )

        interval: int = get_dataframe_period(
            df, by_column=timestamp_column, n=None
        ).seconds

        if interval not in [QUARTER_HOUR, HOUR]:
            raise serializers.ValidationError(
                f"Expected intervals are either 15 or 60 minutes. "
                f"Found '{interval/60}' minutes."
            )

        # Validate that upload file contains interval readings `up to` 366 days
        first_interval: pd.Timestamp = indices[0]
        last_interval: pd.Timestamp = indices[-1]

        span_days = (last_interval - first_interval).days
        if span_days > 366:
            raise serializers.ValidationError(
                "Currently each system-profile data span is limited up to 366 days. "
                f"There are {span_days} days between {first_interval} and {last_interval}."
            )

        # CCAs identify each interval by end-interval datetime, but in our
        # modelings (BEO Project) intervals identified with start datetime.
        # Relabel intervals with their start-datetime instead of end-interval datetime.
        indices = indices - pd.to_timedelta(interval, unit="second")

        try:
            df.set_index(
                keys=indices, verify_integrity=True, drop=True, inplace=True,
            )
        except Exception as e:
            raise serializers.ValidationError(
                "Duplicates, inconsistent or missing interval timestamps. ", e
            )
        df.drop(columns=timestamp_column, inplace=True)
        df.index.rename("index", inplace=True)

        # Convert energy or power readings from one of the expected
        # headers/units  kW, kWh, MW, MWH, GW, or GWH into `Power in kW`.
        value = df.columns[0]
        unit = value.upper()
        interval_per_hour = HOUR / interval
        if "KWH" in unit:
            df[value] *= interval_per_hour
        elif "KW" in unit:
            pass
        elif "MWH" in unit:
            df[value] *= interval_per_hour * 1e3
        elif "MW" in unit:
            df[value] *= 1e3
        elif "GWH" in unit:
            df[value] *= interval_per_hour * 1e6
        elif "GW" in unit:
            df[value] *= 1e6
        else:
            raise serializers.ValidationError(
                f"'{unit}' is not an expected unit for aggregated energy or power values. "
                f"Unit should be one of kW, kWh, MW, MWH, GW, or GWH."
            )
        df.rename(columns={value: "kw"}, inplace=True)

        return df

    @action(methods=("get",), detail=True)
    def download(self, request, pk, *args, **kwargs):
        """
        Downloads the CSV file representation of the `SystemProfile`
        """
        system_profile = self.get_queryset().get(id=pk)
        return download_dataframe(
            system_profile.intervalframe.dataframe,
            index=True,
            filename="system-profile-data.csv",
        )
