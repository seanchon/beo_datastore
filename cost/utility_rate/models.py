from datetime import datetime
from jsonfield import JSONField
from multiprocessing import Pool
import pandas as pd

from django.db import connection, models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.bill import OpenEIRateData, ValidationBill
from beo_datastore.libs.controller import AggregateBillCalculation
from beo_datastore.libs.intervalframe import ValidationFrame288
from beo_datastore.libs.models import ValidationModel
from beo_datastore.libs.views import dataframe_to_html

from reference.reference_model.models import (
    DERSimulation,
    Sector,
    VoltageCategory,
)
from reference.auth_user.models import LoadServingEntity


class RatePlan(ValidationModel):
    """
    A RatePlan is a container for related RateCollections.
    """

    name = models.CharField(max_length=128)
    description = models.TextField(blank=True, null=True)
    demand_min = models.IntegerField(blank=True, null=True)
    demand_max = models.IntegerField(blank=True, null=True)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="rate_plans",
        on_delete=models.PROTECT,
    )
    sector = models.ForeignKey(
        to=Sector, related_name="rate_plans", on_delete=models.PROTECT
    )
    voltage_category = models.ForeignKey(
        to=VoltageCategory,
        related_name="rate_plans",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )

    class Meta:
        ordering = ["id"]

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.load_serving_entity.name + ": " + self.name

    def get_latest_rate_collection(self, start):
        """
        Return latest RateCollection object with effective date less than or
        equal to start.

        :param start: datetime
        :return: RateCollection
        """
        return self.rate_collections.filter(effective_date__lte=start).last()

    def generate_many_bills(
        self, intervalframe, date_ranges, multiprocess=False
    ):
        """
        Generate many ValidationBills based on list of (start, end_limit)
        date range tuples.

        :param intervalframe: ValidationIntervalFrame
        :param date_ranges: list of start, end_limit datetime tuples
        :param multiprocess: True to run as a multiprocess job
        """
        if multiprocess:
            with Pool() as pool:
                bills = pool.starmap(
                    ValidationBill,
                    zip(
                        [
                            intervalframe.filter_by_datetime(start, end_limit)
                            for start, end_limit in date_ranges
                        ],
                        [
                            getattr(
                                self.get_latest_rate_collection(start),
                                "openei_rate_data",
                                OpenEIRateData(rate_data={}),
                            )
                            for start, _ in date_ranges
                        ],
                    ),
                )
        else:
            bills = []
            for start, end_limit in date_ranges:
                bills.append(
                    ValidationBill(
                        intervalframe=intervalframe.filter_by_datetime(
                            start, end_limit
                        ),
                        openei_rate_data=getattr(
                            self.get_latest_rate_collection(start),
                            "openei_rate_data",
                            OpenEIRateData(rate_data={}),
                        ),
                    )
                )

        # return bills in dict with start dates as indices
        return {x[0][0]: x[1] for x in zip(date_ranges, bills)}

    def get_rate_frame288_by_year(
        self, year, rate_type, schedule_type, tier=0
    ):
        """
        Return ValidationFrame288 of combined rates from associated
        rate_collections.

        :param rate_type: choice "energy" or "demand"
        :param schedule_type: choice "weekday" or "weekend"
        :param tier: choice of tier for tiered-rates (integer)
        :return: ValidationFrame288
        """
        frame288_matrix = []
        for month in range(1, 13):
            rate_collection = self.get_latest_rate_collection(
                start=datetime(year, month, 1)
            )
            if not rate_collection:
                frame288_matrix.append([None] * 24)
            else:
                frame288_matrix.append(
                    rate_collection.openei_rate_data.get_rate_frame288(
                        rate_type=rate_type,
                        schedule_type=schedule_type,
                        tier=tier,
                    )
                    .dataframe[month]
                    .values
                )

        return ValidationFrame288.convert_matrix_to_frame288(frame288_matrix)

    @staticmethod
    def get_rate_plan_alias(rate_plan_name):
        """
        Get the rate plan used in RatePlan naming from the rate_plan_name used
        in the Item 17 RS (rate schedule) column.
        """
        if not rate_plan_name:
            return ""

        alias = rate_plan_name

        # remove H2 or H from beginning
        if alias.startswith("H2"):
            alias = alias[2:]
        elif alias.startswith("H"):
            alias = alias[1:]
        # remove X, N, or S from ending
        if alias.endswith("X"):
            alias = alias[:-1]
        if alias.endswith("N"):
            alias = alias[:-1]
        if alias.endswith("S"):
            alias = alias[:-1]
        if alias == "EVA":
            alias = "EV"
        # convert ETOU to TOU
        alias = alias.replace("ETOU", "E-TOU")

        return alias

    @classmethod
    def get_linked_rate_plans(cls, load_serving_entity, rate_plan_name):
        """
        Get RatePlan objects that belong to load_serving_entity associated with
        rate_plan_name.
        """
        rate_plan_alias = cls.get_rate_plan_alias(rate_plan_name)

        if connection.vendor == "sqlite":
            regex = r"\b{}\b".format(rate_plan_alias)
        elif connection.vendor == "postgresql":
            regex = r"\y{}\y".format(rate_plan_alias)
        else:
            regex = r""

        return load_serving_entity.rate_plans.filter(
            models.Q(name__contains=rate_plan_name)
            | models.Q(name__iregex=regex)
        ).distinct()


class RateCollection(ValidationModel):
    """
    A RateCollection is a colletion of rates and TOU lookup tables based on
    data sourced from the OpenEI U.S. Utility Rate Database.

    Source: https://openei.org/apps/USURDB/
    """

    rate_data = JSONField()
    openei_url = models.URLField(max_length=128, blank=True, null=True)
    utility_url = models.URLField(max_length=128)
    effective_date = models.DateField()
    rate_plan = models.ForeignKey(
        to=RatePlan, related_name="rate_collections", on_delete=models.CASCADE
    )

    class Meta:
        ordering = ["effective_date"]

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "{} effective {}".format(self.rate_plan, self.effective_date)

    @property
    def openei_rate_data(self):
        """
        Adds properties from OpenEIRateData container.
        """
        return OpenEIRateData(self.rate_data)

    @cached_property
    def energy_weekday_rates_frame288(self):
        """
        Return ValidationFrame288 representation of weekday energy rates. For
        tiered rates the lowest-tiered rate is used.

        :return: ValidationFrame288
        """
        return self.openei_rate_data.get_rate_frame288(
            rate_type="energy", schedule_type="weekday"
        )

    @property
    def energy_weekday_rates_html_table(self):
        """
        Return Django-formatted HTML energy weekday rates.
        """
        return dataframe_to_html(self.energy_weekday_rates_frame288.dataframe)

    @cached_property
    def energy_weekend_rates_frame288(self):
        """
        Return ValidationFrame288 representation of weekend energy rates. For
        tiered rates the lowest-tiered rate is used.
        """
        return self.openei_rate_data.get_rate_frame288(
            rate_type="energy", schedule_type="weekend"
        )

    @property
    def energy_weekend_rates_html_table(self):
        """
        Return Django-formatted HTML energy weekend rates.
        """
        return dataframe_to_html(self.energy_weekend_rates_frame288.dataframe)

    @cached_property
    def demand_weekday_rates_frame288(self):
        """
        Return ValidationFrame288 representation of weekday demand rates. For
        tiered rates the lowest-tiered rate is used.
        """
        return self.openei_rate_data.get_rate_frame288(
            rate_type="demand", schedule_type="weekday"
        )

    @property
    def demand_weekday_rates_html_table(self):
        """
        Return Django-formatted HTML demand weekday rates.
        """
        return dataframe_to_html(self.demand_weekday_rates_frame288.dataframe)

    @cached_property
    def demand_weekend_rates_frame288(self):
        """
        Return ValidationFrame288 representation of weekend demand rates. For
        tiered rates the lowest-tiered rate is used.
        """
        return self.openei_rate_data.get_rate_frame288(
            rate_type="demand", schedule_type="weekend"
        )

    @property
    def demand_weekend_rates_html_table(self):
        """
        Return Django-formatted HTML demand weekend rates.
        """
        return dataframe_to_html(self.demand_weekend_rates_frame288.dataframe)

    @classmethod
    def all_fixed_rate_keys(cls):
        """
        Return set of fixed-rate keys found in all objects.
        """
        return set().union(
            *[x.openei_rate_data.fixed_rate_keys for x in cls.objects.all()]
        )

    @classmethod
    def all_energy_rate_keys(cls):
        """
        Return set of energy-rate keys found in all objects.
        """
        return set().union(
            *[x.openei_rate_data.energy_rate_keys for x in cls.objects.all()]
        )

    @classmethod
    def all_demand_rate_keys(cls):
        """
        Return set of demand-rate keys found in all objects.
        """
        return set().union(
            *[x.openei_rate_data.demand_rate_keys for x in cls.objects.all()]
        )


class StoredBillCalculation(ValidationModel):
    """
    Container for storing AggregateBillCalculation.
    """

    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()
    der_simulation = models.ForeignKey(
        to=DERSimulation,
        related_name="stored_bill_calculations",
        on_delete=models.CASCADE,
    )
    rate_plan = models.ForeignKey(
        to=RatePlan,
        related_name="stored_bill_calculations",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("der_simulation", "rate_plan")

    @property
    def net_impact(self):
        """
        Return total of all post-DER bill totals minus all pre-DER bill totals.
        """
        return self.post_DER_total - self.pre_DER_total

    @property
    def meter(self):
        return self.der_simulation.meter

    @cached_property
    def date_ranges(self):
        """
        Return date ranges of all bills.
        """
        return [
            (x[0], x[1])
            for x in self.bill_comparisons.order_by("start").values_list(
                "start", "end_limit"
            )
        ]

    @cached_property
    def pre_bills(self):
        """
        Return dictionary of pre-DER bills.
        """
        return {
            self.meter: {
                x.start: x.pre_DER_validation_bill
                for x in self.bill_comparisons.all()
            }
        }

    @cached_property
    def post_bills(self):
        """
        Return dictionary of post-DER bills.
        """
        return {
            self.meter: {
                x.start: x.post_DER_validation_bill
                for x in self.bill_comparisons.all()
            }
        }

    @cached_property
    def aggregate_bill_calculation(self):
        """
        Return AggregateBillCalculation equivalent of self.
        """
        return AggregateBillCalculation(
            agg_simulation=self.der_simulation.agg_simulation,
            rate_plan=self.rate_plan,
            date_ranges=self.date_ranges,
            pre_bills=self.pre_bills,
            post_bills=self.post_bills,
        )

    @staticmethod
    def create_date_ranges(intervalframe):
        """
        Based on a ValidationIntervalFrame, create date ranges representing
        the first and last day of every month detected.

        :param intervalframe: ValidationIntervalFrame
        :return: list of start, end_limit datetime tuples
        """
        date_ranges = []
        for month, year in intervalframe.distinct_month_years:
            if month == 12:
                date_ranges.append(
                    (datetime(year, month, 1), datetime(year + 1, 1, 1))
                )
            else:
                date_ranges.append(
                    (datetime(year, month, 1), datetime(year, month + 1, 1))
                )

        return date_ranges

    @classmethod
    def get_or_create_from_objects(
        cls, der_simulation, rate_plan, multiprocess=False
    ):
        """
        Get existing or create new StoredBillCalculation from a
        DERSimulation and RatePlan.

        Billing date ranges are created automatically from the first to last
        day of every month found in a DERSimulation, which are used
        to created BillComparisons.

        :param der_simulation: DERSimulation
        :param rate_plan: RatePlan
        :param multiprocess: True to multiprocess
        :return: (
            StoredBillCalculation,
            StoredBillCalculation created (True/False)
        )
        """
        with transaction.atomic():
            agg_bill_calculation = AggregateBillCalculation.create(
                agg_simulation=der_simulation.agg_simulation,
                rate_plan=rate_plan,
                date_ranges=cls.create_date_ranges(
                    intervalframe=der_simulation.pre_der_intervalframe
                ),
                multiprocess=multiprocess,
            )
            bill_collection, new = cls.objects.get_or_create(
                pre_DER_total=agg_bill_calculation.pre_DER_total,
                post_DER_total=agg_bill_calculation.post_DER_total,
                der_simulation=der_simulation,
                rate_plan=rate_plan,
            )

            if new:
                for start, end_limit in agg_bill_calculation.date_ranges:
                    pre_DER_total = agg_bill_calculation.pre_bills[
                        der_simulation.id
                    ][start].total
                    post_DER_total = agg_bill_calculation.post_bills[
                        der_simulation.id
                    ][start].total
                    BillComparison.objects.create(
                        start=start,
                        end_limit=end_limit,
                        pre_DER_total=pre_DER_total,
                        post_DER_total=post_DER_total,
                        bill_collection=bill_collection,
                    )

            return (bill_collection, new)

    @classmethod
    def generate(cls, der_simulation_set, rate_plan, multiprocess=False):
        """
        Get or create many StoredBillCalculations at once. Pre-existing
        StoredBillCalculations are retrieved and non-existing
        StoredBillCalculations are created.

        :param der_simulation_set: QuerySet or set of
            DERSimulations
        :param RatePlan: RatePlan
        :param multiprocess: True to multiprocess
        :return: StoredBillCalculation QuerySet
        """
        with transaction.atomic():
            # get existing bill calculations
            stored_bill_calculations = cls.objects.filter(
                der_simulation__in=der_simulation_set, rate_plan=rate_plan
            )

            # create new bill calculations
            existing_der_simulations = [
                x.der_simulation for x in stored_bill_calculations
            ]
            for der_simulation in der_simulation_set:
                if der_simulation not in existing_der_simulations:
                    cls.get_or_create_from_objects(
                        der_simulation=der_simulation,
                        rate_plan=rate_plan,
                        multiprocess=multiprocess,
                    )

            return cls.objects.filter(
                der_simulation__in=der_simulation_set, rate_plan=rate_plan
            )

    @staticmethod
    def get_report(bill_calculations):
        """
        Return pandas DataFrame in the format:

        |   ID  |   BillPreDER  |   BillPostDER |   BillDelta   |

        :param bill_calculations: QuerySet or set of StoredBillCalculations
        :return: pandas DataFrame
        """
        dataframe = pd.DataFrame(
            sorted(
                [
                    (
                        x.der_simulation.meter.id,
                        x.pre_DER_total,
                        x.post_DER_total,
                        x.net_impact,
                    )
                    for x in bill_calculations
                ],
                key=lambda x: x[1],
            )
        )

        if not dataframe.empty:
            return dataframe.rename(
                columns={
                    0: "ID",
                    1: "BillPreDER",
                    2: "BillPostDER",
                    3: "BillDelta",
                }
            ).set_index("ID")
        else:
            return pd.DataFrame()


class BillComparison(ValidationModel):
    """
    Stores only before and after bill totals. Bill details need to be
    generated in order to view details.
    """

    start = models.DateTimeField()
    end_limit = models.DateTimeField()
    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()
    bill_collection = models.ForeignKey(
        to=StoredBillCalculation,
        related_name="bill_comparisons",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("start", "end_limit", "bill_collection")

    @property
    def net_impact(self):
        """
        Return post-DER total minus pre-DER total.
        """
        return self.post_DER_total - self.pre_DER_total

    @cached_property
    def pre_der_intervalframe(self):
        """
        Return pre-DER ValidationIntervalFrame.
        """
        frame = self.bill_collection.der_simulation.pre_der_intervalframe
        return frame.filter_by_datetime(
            start=self.start, end_limit=self.end_limit
        )

    @cached_property
    def post_der_intervalframe(self):
        """
        Return post-DER ValidationIntervalFrame.
        """
        frame = self.bill_collection.der_simulation.post_der_intervalframe
        return frame.filter_by_datetime(
            start=self.start, end_limit=self.end_limit
        )

    @cached_property
    def pre_DER_validation_bill(self):
        """
        Generate pre-DER ValidationBill from scratch.
        """
        return ValidationBill(
            intervalframe=self.pre_der_intervalframe,
            openei_rate_data=(
                getattr(
                    self.bill_collection.rate_plan.get_latest_rate_collection(
                        start=self.start
                    ),
                    "openei_rate_data",
                    OpenEIRateData(rate_data={}),
                )
            ),
        )

    @property
    def pre_DER_bill_html(self):
        """
        Return Django-formatted HTML pre-DER bill.
        """
        return dataframe_to_html(self.pre_DER_validation_bill.total_dataframe)

    @cached_property
    def post_DER_validation_bill(self):
        """
        Generate post-DER ValidationBill from scratch.
        """
        return ValidationBill(
            intervalframe=self.post_der_intervalframe,
            openei_rate_data=(
                getattr(
                    self.bill_collection.rate_plan.get_latest_rate_collection(
                        start=self.start
                    ),
                    "openei_rate_data",
                    OpenEIRateData(rate_data={}),
                )
            ),
        )

    @property
    def post_DER_bill_html(self):
        """
        Return Django-formatted HTML post-DER bill.
        """
        return dataframe_to_html(self.post_DER_validation_bill.total_dataframe)
