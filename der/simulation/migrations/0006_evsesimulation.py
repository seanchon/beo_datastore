# Generated by Django 2.2.7 on 2020-08-04 01:00

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0005_auto_20200804_0054"),
        ("simulation", "0005_auto_20200804_0054"),
    ]

    operations = [
        migrations.CreateModel(
            name="EVSESimulation",
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
            options={"abstract": False, "base_manager_name": "objects"},
            bases=("reference_model.dersimulation",),
        ),
    ]
