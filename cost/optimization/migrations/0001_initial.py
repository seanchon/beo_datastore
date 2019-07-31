# Generated by Django 2.2.2 on 2019-07-31 20:07

from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("utility_rate", "0001_initial"),
        ("customer", "0001_initial"),
        ("ghg", "0001_initial"),
        ("reference_model", "0001_initial"),
        ("simulation", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="SimulationOptimization",
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
                ("start", models.DateTimeField()),
                ("end_limit", models.DateTimeField()),
                (
                    "battery_configuration",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="simulation_optimizations",
                        to="simulation.BatteryConfiguration",
                    ),
                ),
                (
                    "charge_schedule",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="charge_schedule_simulation_optimizations",
                        to="simulation.BatterySchedule",
                    ),
                ),
                (
                    "discharge_schedule",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="discharge_schedule_simulation_optimizations",
                        to="simulation.BatterySchedule",
                    ),
                ),
                (
                    "ghg_rates",
                    models.ManyToManyField(
                        related_name="simulation_optimizations",
                        to="ghg.GHGRate",
                    ),
                ),
                (
                    "load_serving_entity",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="simulation_optimizations",
                        to="reference_model.LoadServingEntity",
                    ),
                ),
                (
                    "meters",
                    models.ManyToManyField(
                        related_name="simulation_optimizations",
                        to="customer.Meter",
                    ),
                ),
                (
                    "rate_plan",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="simulation_optimizations",
                        to="utility_rate.RatePlan",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {
                    (
                        "start",
                        "end_limit",
                        "charge_schedule",
                        "discharge_schedule",
                        "battery_configuration",
                        "load_serving_entity",
                        "rate_plan",
                    )
                },
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="MultiScenarioOptimization",
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
                (
                    "simulation_optimizations",
                    models.ManyToManyField(
                        to="optimization.SimulationOptimization"
                    ),
                ),
            ],
            options={"ordering": ["id"]},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
    ]
