# Generated by Django 2.2.7 on 2020-03-11 22:24

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0001_initial"),
        ("utility_rate", "0001_initial"),
        ("study", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="singlescenariostudy",
            name="meter_group",
            field=models.ForeignKey(
                default=1,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="single_scenario_studies",
                to="reference_model.MeterGroup",
            ),
            preserve_default=False,
        ),
        migrations.AlterUniqueTogether(
            name="singlescenariostudy",
            unique_together={
                (
                    "start",
                    "end_limit",
                    "der_strategy",
                    "der_configuration",
                    "meter_group",
                    "load_serving_entity",
                    "rate_plan",
                )
            },
        ),
        migrations.RemoveField(
            model_name="singlescenariostudy", name="meter_groups"
        ),
    ]
