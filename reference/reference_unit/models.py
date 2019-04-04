from django.db import models

from beo_datastore.libs.models import ValidationModel


class BuildingType(ValidationModel):
    name = models.CharField(max_length=32)
    floor_area = models.IntegerField(blank=False)
    number_of_floors = models.IntegerField(blank=False)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name


class DataUnit(ValidationModel):
    name = models.CharField(max_length=8)

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return self.name
