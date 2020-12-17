# Generated by Django 2.2.7 on 2020-12-09 01:35

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0011_auto_20201105_0320"),
        ("simulation", "0013_drop_solar_parameters_json_uniqueness"),
    ]

    operations = [
        migrations.CreateModel(
            name="FuelSwitchingConfiguration",
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
                ("space_heating", models.BooleanField(default=True)),
                ("water_heating", models.BooleanField(default=True)),
            ],
            options={"verbose_name_plural": "Fuel Switching configurations"},
            bases=("reference_model.derconfiguration",),
        ),
        migrations.CreateModel(
            name="FuelSwitchingSimulation",
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
            ],
            options={
                "verbose_name_plural": "Solar PV simulations",
                "abstract": False,
                "base_manager_name": "objects",
            },
            bases=("reference_model.dersimulation",),
        ),
        migrations.CreateModel(
            name="FuelSwitchingStrategy",
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
            ],
            options={"verbose_name_plural": "Fuel Switching strategies"},
            bases=("reference_model.derstrategy",),
        ),
        migrations.AlterModelOptions(
            name="batteryconfiguration",
            options={"verbose_name_plural": "Battery configurations"},
        ),
        migrations.AlterModelOptions(
            name="evseconfiguration",
            options={"verbose_name_plural": "EVSE configurations"},
        ),
        migrations.AlterModelOptions(
            name="solarpvconfiguration",
            options={"verbose_name_plural": "Solar PV configurations"},
        ),
    ]
