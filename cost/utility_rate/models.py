from jsonfield import JSONField

from django.db import models

from beo_datastore.libs.models import ValidationModel

from reference.reference_model.models import (
    RateUnit,
    Sector,
    Utility,
    VoltageCategory,
)


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

    @property
    def fixed_rate_keys(self):
        """
        Return set of fixed-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "fixed" in x}

    @property
    def fixed_rates(self):
        """
        Return fixed rates applied on a per day, per month, etc. basis.
        """
        return self.rate_data.get("fixedKeyVals", [])

    @property
    def fixed_rate_unit(self):
        """
        Return fixed rate unit (ex. $/day, $/month).
        """
        fixed_charge_units = self.rate_data.get("fixedChargeUnits", None)

        # defaults to $/day
        if fixed_charge_units is None:
            return RateUnit.objects.get(
                numerator__name="$", denominator__name="day"
            )
        else:
            return RateUnit.objects.get(
                numerator__name=fixed_charge_units.split("/")[0],
                denominator__name=fixed_charge_units.split("/")[1],
            )

    @property
    def fixed_meter_charge(self):
        """
        Return fixed meter charge per bill ($/meter).
        """
        return self.rate_data.get("fixedChargeFirstMeter", 0)

    @property
    def energy_rate_keys(self):
        """
        Return set of energy-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "energy" in x}

    @property
    def demand_rate_keys(self):
        """
        Return set of demand-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "demand" in x}

    @classmethod
    def all_fixed_rate_keys(cls):
        """
        Return set of fixed-rate keys found in all objects.
        """
        return set().union(*[x.fixed_rate_keys for x in cls.objects.all()])

    @classmethod
    def all_energy_rate_keys(cls):
        """
        Return set of energy-rate keys found in all objects.
        """
        return set().union(*[x.energy_rate_keys for x in cls.objects.all()])

    @classmethod
    def all_demand_rate_keys(cls):
        """
        Return set of demand-rate keys found in all objects.
        """
        return set().union(*[x.demand_rate_keys for x in cls.objects.all()])
