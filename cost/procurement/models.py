from datetime import datetime, timedelta
from functools import reduce
from jsonfield import JSONField
import os
import pandas as pd
from pyoasis.report import OASISReport
from pyoasis.utils import create_oasis_url, download_files
from pytz import timezone
import uuid

from django.db import models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.controller import (
    AggregateProcurementCostCalculation,
    AggregateResourceAdequacyCalculation,
)
from beo_datastore.libs.dataframe import get_dataframe_period
from beo_datastore.libs.intervalframe import ValidationFrame288
from beo_datastore.libs.intervalframe_file import (
    ArbitraryDataFrameFile,
    PowerIntervalFrameFile,
)
from beo_datastore.libs.models import ValidationModel, IntervalFrameFileMixin
from beo_datastore.libs.plot_intervalframe import (
    plot_frame288,
    plot_intervalframe,
)
from beo_datastore.libs.procurement import ProcurementRateIntervalFrame
from beo_datastore.settings import MEDIA_ROOT
from beo_datastore.libs.views import dataframe_to_html

from reference.reference_model.models import DERSimulation, LoadServingEntity

# File constants
RA_DOLLARS_PER_KW = 6


class SystemProfileIntervalFrame(PowerIntervalFrameFile):
    """
    Model for handling SystemProfile IntervalFrameFiles.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "system_profiles")


class SystemProfile(IntervalFrameFileMixin, ValidationModel):
    name = models.CharField(max_length=32)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="system_profiles",
        on_delete=models.PROTECT,
    )

    # Required by IntervalFrameFileMixin.
    frame_file_class = SystemProfileIntervalFrame

    class Meta:
        ordering = ["id"]
        unique_together = ["name", "load_serving_entity"]

    def __str__(self):
        return self.load_serving_entity.name + ": " + self.name

    @property
    def short_name(self):
        """
        Name minus whitespace.
        """
        return self.name.replace(" ", "")

    @property
    def average_frame288_html_plot(self):
        """
        Return Django-formatted HTML average 288 plt.
        """
        return plot_frame288(
            frame288=ValidationFrame288(
                self.intervalframe.average_frame288.dataframe / 1000
            ),
            y_label="MW",
            to_html=True,
        )

    @property
    def maximum_frame288_html_plot(self):
        """
        Return Django-formatted HTML maximum 288 plt.
        """
        return plot_frame288(
            frame288=ValidationFrame288(
                self.intervalframe.maximum_frame288.dataframe / 1000
            ),
            y_label="MW",
            to_html=True,
        )


class StoredResourceAdequacyCalculation(ValidationModel):
    """
    Container for storing AggregateResourceAdequacyCalculation.
    """

    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()
    der_simulation = models.ForeignKey(
        to=DERSimulation,
        related_name="stored_resource_adequacy_calculations",
        on_delete=models.CASCADE,
    )
    system_profile = models.ForeignKey(
        to=SystemProfile,
        related_name="stored_resource_adequacy_calculations",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("der_simulation", "system_profile")

    @property
    def net_impact(self):
        """
        Return post-DER total minus pre-DER total.
        """
        return self.post_DER_total - self.pre_DER_total

    @property
    def pre_der_total_cost(self):
        """
        Return pre-DER total kW multiplied by $/kW RA equivalency
        """
        return self.pre_DER_total * RA_DOLLARS_PER_KW

    @property
    def post_der_total_cost(self):
        """
        Return post-DER total kW multiplied by $/kW RA equivalency
        """
        return self.post_DER_total * RA_DOLLARS_PER_KW

    @property
    def net_impact_cost(self):
        """
        Return post-DER total cost minus pre-DER total cost.
        """
        return self.post_der_total_cost - self.pre_der_total_cost

    @property
    def comparision_html_table(self):
        """
        Return Django-formatted HTML pre vs. post comparision table.
        """
        return dataframe_to_html(
            self.aggregate_resource_adequacy_calculation.comparison_table
        )

    @cached_property
    def aggregate_resource_adequacy_calculation(self):
        """
        Return AggregateResourceAdequacyCalculation equivalent of self.
        """
        return AggregateResourceAdequacyCalculation(
            agg_simulation=self.der_simulation.agg_simulation,
            system_profile_intervalframe=self.system_profile.intervalframe,
        )

    @classmethod
    def generate(cls, der_simulation_set, system_profile):
        """
        Get or create many StoredResourceAdequacyCalculations at once.
        Pre-existing StoredResourceAdequacyCalculations are retrieved and
        non-existing StoredResourceAdequacyCalculations are created.

        :param der_simulation_set: QuerySet or set of
            DERSimulations
        :param system_profile: SystemProfile
        :return: StoredResourceAdequacyCalculation QuerySet
        """
        with transaction.atomic():
            # get existing RA calculations
            stored_ra_calculations = cls.objects.filter(
                der_simulation__in=der_simulation_set,
                system_profile=system_profile,
            )

            # create new RA calculations
            stored_simulations = [
                x.der_simulation for x in stored_ra_calculations
            ]
            objects = []
            for der_simulation in der_simulation_set:
                if der_simulation in stored_simulations:
                    continue
                ra_calculation = AggregateResourceAdequacyCalculation(
                    agg_simulation=der_simulation.agg_simulation,
                    system_profile_intervalframe=system_profile.intervalframe,
                )
                objects.append(
                    cls(
                        pre_DER_total=ra_calculation.pre_DER_total,
                        post_DER_total=ra_calculation.post_DER_total,
                        der_simulation=der_simulation,
                        system_profile=system_profile,
                    )
                )
            cls.objects.bulk_create(objects)

            return cls.objects.filter(
                der_simulation__in=der_simulation_set,
                system_profile=system_profile,
            )

    @staticmethod
    def get_report(resource_adequacy_calculations):
        """
        Return pandas DataFrame in the format:

        |   ID  |   RAPreDER    |   RAPostDER   |   RADelta |

        :param resource_adequacy_calculations: QuerySet or set of
            StoredResourceAdequacyCalculations
        :return: pandas DataFrame
        """
        system_profile_ids = (
            resource_adequacy_calculations.values_list(
                "system_profile", flat=True
            )
            .order_by()
            .distinct()
        )

        dataframes = []
        for system_profile_id in system_profile_ids:
            title_prefix = ""
            system_profile = SystemProfile.objects.get(id=system_profile_id)
            if len(system_profile_ids) > 1:
                title_prefix = system_profile.short_name

            dataframe = pd.DataFrame(
                sorted(
                    [
                        (
                            x.der_simulation.meter.id,
                            x.pre_DER_total,
                            x.post_DER_total,
                            x.net_impact,
                            x.pre_der_total_cost,
                            x.post_der_total_cost,
                            x.net_impact_cost,
                        )
                        for x in resource_adequacy_calculations.filter(
                            system_profile=system_profile
                        )
                    ],
                    key=lambda x: x[1],
                )
            )

            if not dataframe.empty:
                dataframes.append(
                    dataframe.rename(
                        columns={
                            0: "ID",
                            1: "{}RAPreDER".format(title_prefix),
                            2: "{}RAPostDER".format(title_prefix),
                            3: "{}RADelta".format(title_prefix),
                            4: "{}RAPreDERCost".format(title_prefix),
                            5: "{}RAPostDERCost".format(title_prefix),
                            6: "{}RADeltaCost".format(title_prefix),
                        }
                    ).set_index("ID")
                )

        return reduce(
            lambda x, y: x.join(y, how="outer", lsuffix="_0", rsuffix="_1"),
            dataframes,
            pd.DataFrame(),
        )


class CAISOReportDataFrame(ArbitraryDataFrameFile):
    """
    Model for storing a CAISO OASIS Report to file.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "energy_rates")


class CAISOReport(IntervalFrameFileMixin, ValidationModel):
    """
    CAISO OASIS Report.

    CAISO will not return a full year's worth of data, so it is necessary to
    piece together an entire year's worth of data from multiple calls.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    report_name = models.CharField(max_length=32)
    query_params = JSONField()
    year = models.IntegerField()

    # Required by IntervalFrameFileMixin.
    frame_file_class = CAISOReportDataFrame

    class Meta:
        ordering = ["id"]
        unique_together = ["report_name", "query_params", "year"]

    @property
    def report(self):
        return self.frame.dataframe

    @classmethod
    def get_or_create(
        cls,
        report_name,
        year,
        query_params,
        overwrite=False,
        chunk_size=timedelta(days=1),
        max_attempts=3,
        destination_directory="caiso_downloads",
        timezone_=timezone("US/Pacific"),
    ):
        """
        Get or create CAISOReport and fetch reports from OASIS if necessary.

        :param report_name: see pyoasis.utils.get_report_names()
        :param year: int
        :param query_params: see pyoasis.utils.get_report_params()
        :param overwrite: True to fetch new reports (default: False)
        :param chunk_size: length of report to request (timedelta)
        :param max_attempts: number of back-off attempts (int)
        :param destination_directory: directory to store temporary files
        :param timezone_: pytz.timezone object used for naive start and
            end_limit datetime objects
        :return: CAISOReport
        """
        with transaction.atomic():
            caiso_report, created = cls.objects.get_or_create(
                report_name=report_name, query_params=query_params, year=year
            )

            if caiso_report.report.empty or overwrite:
                caiso_report.intervalframe.dataframe = cls.fetch_report(
                    report_name=report_name,
                    start=datetime(year, 1, 1),
                    end_limit=datetime(year + 1, 1, 1),
                    query_params=query_params,
                    chunk_size=chunk_size,
                    max_attempts=max_attempts,
                    destination_directory=destination_directory,
                    timezone_=timezone_,
                )
                caiso_report.save()

            return caiso_report, created

    @staticmethod
    def fetch_report(
        report_name,
        start,
        end_limit,
        query_params,
        chunk_size=timedelta(days=1),
        max_attempts=3,
        destination_directory="caiso_downloads",
        timezone_=timezone("US/Pacific"),
        start_column="INTERVAL_START_GMT",
        end_column="INTERVAL_END_GMT",
        sort_by=["DATA_ITEM", "INTERVAL_START_GMT"],
    ):
        """
        Fetch reports from OASIS and stitch together to create a single report
        beginning on start and ending on end_limit.

        :param report_name: see pyoasis.utils.get_report_names()
        :param start: datetime
        :param end_limit: datetime
        :param query_params: see pyoasis.utils.get_report_params()
        :param chunk_size: length of report to request (timedelta)
        :param max_attempts: number of back-off attempts (int)
        :param destination_directory: directory to store temporary files
        :param timezone_: pytz.timezone object used for naive start and
            end_limit datetime objects
        :param start_column: column name of start timestamps
        :param end_column: column name of end timestamps
        :param sort_by: sort order of resultant dataframe
        :return: DataFrame
        """
        report_dataframe = pd.DataFrame()

        # localize naive datetime
        if not start.tzinfo:
            start = timezone_.localize(start)
        if not end_limit.tzinfo:
            end_limit = timezone_.localize(end_limit)

        chunk_start = start
        chunk_end = chunk_start + chunk_size
        while chunk_end < end_limit + chunk_size:
            url = create_oasis_url(
                report_name=report_name,
                start=chunk_start,
                end=chunk_end,
                query_params=query_params,
            )
            file_locations = download_files(
                url=url,
                destination_directory=destination_directory,
                max_attempts=max_attempts,
            )
            for file_location in file_locations:
                oasis_report = OASISReport(file_location)
                if hasattr(oasis_report, "report_dataframe"):
                    report_dataframe = report_dataframe.append(
                        oasis_report.report_dataframe
                    )
                os.remove(file_location)

            chunk_start = chunk_end
            chunk_end = chunk_end + chunk_size

        report_dataframe[start_column] = pd.to_datetime(
            report_dataframe[start_column]
        )
        report_dataframe[end_column] = pd.to_datetime(
            report_dataframe[end_column]
        )

        sorted_frame = report_dataframe[
            (report_dataframe[start_column] >= start)
            & (report_dataframe[end_column] <= end_limit)
        ].sort_values(by=sort_by)

        # Check for data gaps
        period = get_dataframe_period(sorted_frame.set_index(start_column))
        date_range = pd.date_range(
            start, end_limit, freq=period, closed="left"
        )

        dates_present = date_range.isin(sorted_frame[start_column])
        if date_range[~dates_present].size > 0:
            raise RuntimeError("OASIS report has missing intervals")

        return sorted_frame

    def get_procurement_rate_intervalframe(
        self,
        index_col="INTERVAL_START_GMT",
        rate_col="VALUE",
        filters={"DATA_ITEM": "LMP_PRC"},
        timezone_=timezone("US/Pacific"),
    ):
        """
        Converts self.report into a ProcurementRateIntervalFrame.

        Defaults to index on INTERVAL_START_GMT, rate on VALUE, and filtering
        on DATA_ITEM equals LMP_PRC.

        :param index_col: column containing start timestamps
        :param rate_col: column containing rates
        :param filters: key/value pair of column/value for filtering
            report
        :param timezone_: timezone object
        :return: ProcurementRateIntervalFrame
        """
        if (
            index_col not in self.report.columns
            or rate_col not in self.report.columns
        ):
            return ProcurementRateIntervalFrame()

        df = self.report.copy()

        # filter report
        filters = {k: v for k, v in filters.items() if k in df.columns}
        # https://stackoverflow.com/questions/34157811
        df = df.loc[(df[list(filters)] == pd.Series(filters)).all(axis=1)]

        # keep index_col and rate_col
        df = df[[index_col, rate_col]]

        # convert rate_col from $/MWh to $/kWh
        df[rate_col] = df[rate_col].astype(float)
        df[rate_col] = df[rate_col] / 1000
        df = df.rename(columns={rate_col: "$/kwh"})

        # convert index_col to PDT and drop timezone from timestamp
        df[index_col] = pd.to_datetime(df[index_col])
        df = df.rename(columns={index_col: "start"})
        df = df.set_index("start")
        df.index = df.index.tz_convert(timezone_).tz_localize(None)

        # drop duplicate index daylight savings
        # https://stackoverflow.com/questions/13035764
        df = df.loc[~df.index.duplicated(keep="first")]

        return ProcurementRateIntervalFrame(dataframe=df)


class CAISORate(ValidationModel):
    """
    Container for referencing associated CAISO ProcurementRateIntervalFrame.
    """

    filters = JSONField()
    caiso_report = models.ForeignKey(
        to=CAISOReport, related_name="caiso_rates", on_delete=models.CASCADE
    )

    class Meta:
        ordering = ["id"]
        unique_together = ["filters", "caiso_report"]

    @property
    def name(self):
        return "{} {}".format(
            self.caiso_report.report_name, self.caiso_report.year
        )

    @property
    def short_name(self):
        return self.name.replace(" ", "")

    @property
    def intervalframe(self):
        """
        Associated CAISO ProcurementRateIntervalFrame.
        """
        return self.caiso_report.get_procurement_rate_intervalframe(
            filters=self.filters
        )

    @property
    def intervalframe_plot(self):
        return plot_intervalframe(self.intervalframe, to_html=True)


class StoredProcurementCostCalculation(ValidationModel):
    """
    Container for storing AggregateProcurementCostCalculation.
    """

    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()
    der_simulation = models.ForeignKey(
        to=DERSimulation,
        related_name="stored_procurement_calculations",
        on_delete=models.CASCADE,
    )
    caiso_rate = models.ForeignKey(
        to=CAISORate,
        related_name="stored_procurement_calculations",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("der_simulation", "caiso_rate")

    @property
    def net_impact(self):
        """
        Return post-DER total minus pre-DER total.
        """
        return self.post_DER_total - self.pre_DER_total

    @cached_property
    def procurement_rate_intervalframe(self):
        return self.caiso_rate.intervalframe

    @cached_property
    def aggregate_procurement_cost_calculation(self):
        """
        Return AggregateProcurementCostCalculation equivalent of self.
        """
        return AggregateProcurementCostCalculation(
            agg_simulation=self.der_simulation.agg_simulation,
            procurement_rate_intervalframe=self.procurement_rate_intervalframe,
        )

    @classmethod
    def generate(cls, der_simulation_set, caiso_rate):
        """
        Get or create many StoredProcurementCostCalculations at once.
        Pre-existing StoredProcurementCostCalculations are retrieved and
        non-existing StoredProcurementCostCalculations are created.

        :param der_simulation_set: QuerySet or set of DERSimulations
        :param caiso_rate: CAISORate
        :return: StoredProcurementCostCalculation QuerySet
        """
        with transaction.atomic():
            # get stored procurement cost calculations
            stored_procurement_calculations = cls.objects.filter(
                der_simulation__in=der_simulation_set, caiso_rate=caiso_rate
            )

            # create new procurement cost calculations
            stored_simulations = [
                x.der_simulation for x in stored_procurement_calculations
            ]
            objects = []
            for der_simulation in der_simulation_set:
                if der_simulation in stored_simulations:
                    continue
                cost_calulation = AggregateProcurementCostCalculation(
                    agg_simulation=der_simulation.agg_simulation,
                    procurement_rate_intervalframe=caiso_rate.intervalframe,
                )
                objects.append(
                    cls(
                        pre_DER_total=cost_calulation.pre_DER_total,
                        post_DER_total=cost_calulation.post_DER_total,
                        der_simulation=der_simulation,
                        caiso_rate=caiso_rate,
                    )
                )
            cls.objects.bulk_create(objects)

            return cls.objects.filter(
                der_simulation__in=der_simulation_set, caiso_rate=caiso_rate
            )

    @staticmethod
    def get_report(procurement_calculations):
        """
        Return pandas DataFrame in the format:

        |   ID  |   CAISOPreDER   |   CAISOPostDER  |   CAISODelta    |

        :param procurement_calculations: QuerySet or set of
            StoredProcurementCostCalculations
        :return: pandas DataFrame
        """
        caiso_rate_ids = (
            procurement_calculations.values_list("caiso_rate", flat=True)
            .order_by()
            .distinct()
        )

        dataframes = []
        for caiso_rate_id in caiso_rate_ids:
            caiso_rate = CAISORate.objects.get(id=caiso_rate_id)

            dataframe = pd.DataFrame(
                sorted(
                    [
                        (
                            x.der_simulation.meter.id,
                            x.pre_DER_total,
                            x.post_DER_total,
                            x.net_impact,
                        )
                        for x in procurement_calculations.filter(
                            caiso_rate=caiso_rate
                        )
                    ],
                    key=lambda x: x[1],
                )
            )

            if not dataframe.empty:
                dataframes.append(
                    dataframe.rename(
                        columns={
                            0: "ID",
                            1: "{}PreDER".format(caiso_rate.short_name),
                            2: "{}PostDER".format(caiso_rate.short_name),
                            3: "{}Delta".format(caiso_rate.short_name),
                        }
                    ).set_index("ID")
                )

        return reduce(
            lambda x, y: x.join(y, how="outer", lsuffix="_0", rsuffix="_1"),
            dataframes,
            pd.DataFrame(),
        )
