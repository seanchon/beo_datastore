# Generated by Django 2.2.7 on 2019-11-11 23:59

import beo_datastore.libs.models
import django.core.validators
from django.db import migrations, models
import django.db.models.deletion
import localflavor.us.models


class Migration(migrations.Migration):

    initial = True

    dependencies = [("reference_model", "0001_initial")]

    operations = [
        migrations.CreateModel(
            name="ReferenceBuilding",
            fields=[
                (
                    "meterintervalframe_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="reference_model.MeterIntervalFrame",
                    ),
                ),
                ("location", models.CharField(max_length=64)),
                (
                    "state",
                    localflavor.us.models.USStateField(
                        blank=True, max_length=2
                    ),
                ),
                (
                    "TMY3_id",
                    models.IntegerField(
                        db_index=True,
                        validators=[
                            django.core.validators.MinValueValidator(100000),
                            django.core.validators.MaxValueValidator(999999),
                        ],
                    ),
                ),
                ("source_file_url", models.URLField(max_length=254)),
                (
                    "building_type",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="reference_buildings",
                        to="reference_model.BuildingType",
                    ),
                ),
                (
                    "data_unit",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="reference_buildings",
                        to="reference_model.DataUnit",
                    ),
                ),
            ],
            options={"ordering": ["id"]},
            bases=(
                beo_datastore.libs.models.IntervalFrameFileMixin,
                "reference_model.meterintervalframe",
            ),
        )
    ]
