from jsonfield import JSONField
from multiprocessing import Pool

from django.db import models

from beo_datastore.libs.bill import OpenEIRateData, ValidationBill
from beo_datastore.libs.models import ValidationModel

from reference.reference_model.models import Sector, Utility, VoltageCategory


class RatePlan(ValidationModel):
    """
    A RatePlan is a container for related RateCollections.
    """

    name = models.CharField(max_length=128)
    description = models.TextField(blank=True, null=True)
    demand_min = models.IntegerField(blank=True, null=True)
    demand_max = models.IntegerField(blank=True, null=True)
    utility = models.ForeignKey(
        Utility, related_name="rate_plans", on_delete=models.PROTECT
    )
    sector = models.ForeignKey(
        Sector, related_name="rate_plans", on_delete=models.PROTECT
    )
    voltage_category = models.ForeignKey(
        VoltageCategory,
        related_name="rate_plans",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name

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
        :param date_ranges: list of (start, end_limit) tuples
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
                            self.get_latest_rate_collection(
                                start
                            ).openei_rate_data
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
                        openei_rate_data=self.get_latest_rate_collection(
                            start
                        ).openei_rate_data,
                    )
                )

        # return bills in dict with start dates as indices
        return {x[0][0]: x[1] for x in zip(date_ranges, bills)}


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
        RatePlan, related_name="rate_collections", on_delete=models.CASCADE
    )

    class Meta:
        ordering = ["effective_date"]

    def __str__(self):
        return "{} effective {}".format(self.rate_plan, self.effective_date)

    @property
    def openei_rate_data(self):
        """
        Adds properties from OpenEIRateData container.
        """
        return OpenEIRateData(self.rate_data)

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
