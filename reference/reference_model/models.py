from django.db import models

from beo_datastore.libs.models import ValidationModel


class BuildingType(ValidationModel):
    """
    U.S. Department of Energy Commercial Reference Buildings.

    https://www.energy.gov/eere/buildings/commercial-reference-buildings
    """

    name = models.CharField(max_length=32)
    floor_area = models.IntegerField(blank=False)
    number_of_floors = models.IntegerField(blank=False)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name


class DataUnit(ValidationModel):
    """
    Units of measure.

    Ex. kw, kwh, therms
    """

    name = models.CharField(max_length=8)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name


class Utility(ValidationModel):
    """
    Investor Owned Utility.
    """

    name = models.CharField(max_length=32)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name


class VoltageCategory(ValidationModel):
    """
    Utility's Customer Voltage Category.
    """

    name = models.CharField(max_length=32)
    utility = models.ForeignKey(
        Utility, related_name="voltage_categories", on_delete=models.PROTECT
    )

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name


class Sector(ValidationModel):
    """
    Classification of Utility customer.

    Ex. Residential, Commercial
    """

    name = models.CharField(max_length=32)
    utility = models.ForeignKey(
        Utility, related_name="sectors", on_delete=models.PROTECT
    )

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name
