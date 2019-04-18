from jsonfield import JSONField

from django.db import models

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


class RateCollection(ValidationModel):
    """
    A RateCollection is a colletion of rates and TOU lookup tables based on
    data sourced from the OpenEI U.S. Utility Rate Database.

    Source: https://openei.org/apps/USURDB/
    """

    rate_data = JSONField()
    openei_url = models.URLField(max_length=128)
    utility_url = models.URLField(max_length=128)
    effective_date = models.DateField()
    rate_plan = models.ForeignKey(
        RatePlan, related_name="rate_collections", on_delete=models.CASCADE
    )

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return "{} effective {}".format(self.rate_plan, self.effective_date)
