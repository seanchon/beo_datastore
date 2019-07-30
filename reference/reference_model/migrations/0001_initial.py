# Generated by Django 2.2.2 on 2019-07-30 21:53

from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr
import localflavor.us.models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DataUnit",
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
                ("name", models.CharField(max_length=8, unique=True)),
            ],
            options={"ordering": ["id"]},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="LoadServingEntity",
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
                ("name", models.CharField(max_length=32, unique=True)),
                ("state", localflavor.us.models.USStateField(max_length=2)),
            ],
            options={"ordering": ["id"]},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="BuildingType",
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
                ("name", models.CharField(max_length=32)),
                ("floor_area", models.IntegerField()),
                ("number_of_floors", models.IntegerField()),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {
                    ("name", "floor_area", "number_of_floors")
                },
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="VoltageCategory",
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
                ("name", models.CharField(max_length=32)),
                (
                    "load_serving_entity",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="voltage_categories",
                        to="reference_model.LoadServingEntity",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {("name", "load_serving_entity")},
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="Sector",
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
                ("name", models.CharField(max_length=32)),
                (
                    "load_serving_entity",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="sectors",
                        to="reference_model.LoadServingEntity",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {("name", "load_serving_entity")},
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="RateUnit",
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
                    "denominator",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="rate_unit_denominators",
                        to="reference_model.DataUnit",
                    ),
                ),
                (
                    "numerator",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="rate_unit_numerators",
                        to="reference_model.DataUnit",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {("numerator", "denominator")},
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
    ]
