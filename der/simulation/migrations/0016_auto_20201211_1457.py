# Generated by Django 2.2.7 on 2020-12-11 14:57

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("simulation", "0015_drop_evse_configuration_fields"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="fuelswitchingsimulation",
            options={
                "base_manager_name": "objects",
                "verbose_name_plural": "Fuel Switching simulations",
            },
        ),
        migrations.AddField(
            model_name="fuelswitchingconfiguration",
            name="performance_coefficient",
            field=models.FloatField(
                default=3.0,
                validators=[django.core.validators.MinValueValidator(0)],
            ),
        ),
    ]
