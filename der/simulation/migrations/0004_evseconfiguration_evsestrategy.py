# Generated by Django 2.2.7 on 2020-07-30 21:50

import django.core.validators
from django.db import migrations, models
import django.db.models.deletion

from beo_datastore.libs.storages import media_migration_helper
from der.simulation.models import DERSchedule


def relocate_der_schedules(apps, schema_editor):
    """
    Relocate media related to DERSchedules following the model's renaming and
    its new `file_directory`
    """

    old_path = "battery_simulations/BatteryScheduleFrame288_{id}.parquet"
    new_path = "der_schedules/DERScheduleFrame288_{id}.parquet"

    for schedule in DERSchedule.objects.all():
        try:
            media_migration_helper.migrate_file(
                old_path.format(id=schedule.id),
                new_path.format(id=schedule.id),
            )
        except Exception as e:
            print(e)


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0003_derstrategy_objective"),
        ("simulation", "0003_batteryschedule_hash"),
    ]

    operations = [
        migrations.CreateModel(
            name="EVSEConfiguration",
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
                (
                    "ev_mpkwh",
                    models.FloatField(
                        validators=[
                            django.core.validators.MinValueValidator(
                                limit_value=0
                            )
                        ]
                    ),
                ),
                (
                    "ev_mpg_eq",
                    models.FloatField(
                        validators=[
                            django.core.validators.MinValueValidator(
                                limit_value=0
                            )
                        ]
                    ),
                ),
                (
                    "ev_capacity",
                    models.FloatField(
                        validators=[
                            django.core.validators.MinValueValidator(
                                limit_value=0
                            )
                        ]
                    ),
                ),
                (
                    "ev_efficiency",
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
                (
                    "evse_rating",
                    models.FloatField(
                        validators=[
                            django.core.validators.MinValueValidator(
                                limit_value=0
                            )
                        ]
                    ),
                ),
                (
                    "ev_count",
                    models.IntegerField(
                        validators=[
                            django.core.validators.MinValueValidator(
                                limit_value=0
                            )
                        ]
                    ),
                ),
                (
                    "evse_count",
                    models.IntegerField(
                        validators=[
                            django.core.validators.MinValueValidator(
                                limit_value=0
                            )
                        ]
                    ),
                ),
            ],
            options={
                "verbose_name_plural": "EVSE configurations",
                "ordering": ["id"],
                "unique_together": {
                    (
                        "ev_mpkwh",
                        "ev_mpg_eq",
                        "ev_capacity",
                        "ev_efficiency",
                        "evse_rating",
                        "ev_count",
                        "evse_count",
                    )
                },
            },
            bases=("reference_model.derconfiguration",),
        ),
        migrations.CreateModel(
            name="EVSEStrategy",
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
                        related_name="charge_schedule_evse_strategies",
                        to="simulation.BatterySchedule",
                    ),
                ),
                (
                    "drive_schedule",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="drive_schedule_evse_strategies",
                        to="simulation.BatterySchedule",
                    ),
                ),
            ],
            options={
                "verbose_name_plural": "EVSE strategies",
                "ordering": ["id"],
                "unique_together": {("charge_schedule", "drive_schedule")},
            },
            bases=("reference_model.derstrategy",),
        ),
        migrations.RenameModel(
            old_name="BatterySchedule", new_name="DERSchedule",
        ),
        migrations.RunPython(
            relocate_der_schedules, reverse_code=migrations.RunPython.noop
        ),
    ]