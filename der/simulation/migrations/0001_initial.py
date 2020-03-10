# Generated by Django 2.2.7 on 2020-03-10 00:43

import beo_datastore.libs.models
import django.core.validators
from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr


class Migration(migrations.Migration):

    initial = True

    dependencies = [("reference_model", "0001_initial")]

    operations = [
        migrations.CreateModel(
            name="BatterySchedule",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("hash", models.CharField(max_length=64, unique=True)),
            ],
            options={"ordering": ["id"]},
            bases=(
                beo_datastore.libs.models.Frame288FileMixin,
                django_auto_repr.AutoRepr,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="StoredBatterySimulation",
            fields=[
                (
                    "dersimulation_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="reference_model.DERSimulation",
                    ),
                ),
                ("pre_DER_total", models.FloatField()),
                ("post_DER_total", models.FloatField()),
            ],
            options={"abstract": False, "base_manager_name": "objects"},
            bases=(
                beo_datastore.libs.models.IntervalFrameFileMixin,
                "reference_model.dersimulation",
            ),
        ),
        migrations.CreateModel(
            name="BatteryConfiguration",
            fields=[
                (
                    "derconfiguration_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="reference_model.DERConfiguration",
                    ),
                ),
                ("rating", models.IntegerField()),
                ("discharge_duration_hours", models.IntegerField()),
                (
                    "efficiency",
                    models.FloatField(
                        validators=[
                            django.core.validators.MinValueValidator(
                                limit_value=0
                            ),
                            django.core.validators.MaxValueValidator(
                                limit_value=1
                            ),
                        ]
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {
                    ("rating", "discharge_duration_hours", "efficiency")
                },
            },
            bases=("reference_model.derconfiguration",),
        ),
        migrations.CreateModel(
            name="BatteryStrategy",
            fields=[
                (
                    "derstrategy_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="reference_model.DERStrategy",
                    ),
                ),
                (
                    "charge_schedule",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="charge_schedule_battery_strategies",
                        to="simulation.BatterySchedule",
                    ),
                ),
                (
                    "discharge_schedule",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="discharge_schedule_battery_strategies",
                        to="simulation.BatterySchedule",
                    ),
                ),
            ],
            options={
                "verbose_name_plural": "battery strategies",
                "ordering": ["id"],
                "unique_together": {("charge_schedule", "discharge_schedule")},
            },
            bases=("reference_model.derstrategy",),
        ),
    ]
