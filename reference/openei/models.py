from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES

from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models

from beo_datastore.libs.models import ValidationModel


class BuildingType(ValidationModel):
    name = models.CharField(max_length=32)
    floor_area = models.IntegerField(blank=False)
    number_of_floors = models.IntegerField(blank=False)

    def __str__(self):
        return self.name


class ReferenceBuilding(ValidationModel):
    location = models.CharField(max_length=64, blank=False)
    state = USStateField(choices=STATE_CHOICES, blank=True)
    TMY3_id = models.IntegerField(
        db_index=True,
        validators=[MinValueValidator(100000), MaxValueValidator(999999)],
    )
    source_file = models.URLField(max_length=254)
    building_type = models.ForeignKey(
        BuildingType,
        related_name="reference_buildings",
        on_delete=models.PROTECT,
    )

    def __str__(self):
        return self.building_type.name + ": " + self.location
