# Generated by Django 2.2.7 on 2020-10-08 23:07

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("study", "0003_remove_scenario_meters"),
    ]

    operations = [
        migrations.AlterField(
            model_name="scenario",
            name="rate_plan",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="scenarios",
                to="utility_rate.RatePlan",
            ),
        ),
    ]
