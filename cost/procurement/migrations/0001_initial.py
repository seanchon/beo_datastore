# Generated by Django 2.2.7 on 2020-03-04 22:33

import beo_datastore.libs.models
from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr


class Migration(migrations.Migration):

    initial = True

    dependencies = [("reference_model", "0001_initial")]

    operations = [
        migrations.CreateModel(
            name="SystemProfile",
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
                        related_name="system_profiles",
                        to="reference_model.LoadServingEntity",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {("name", "load_serving_entity")},
            },
            bases=(
                beo_datastore.libs.models.IntervalFrameFileMixin,
                django_auto_repr.AutoRepr,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="StoredResourceAdequacyCalculation",
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
                ("pre_DER_total", models.FloatField()),
                ("post_DER_total", models.FloatField()),
                (
                    "der_simulation",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="stored_resource_adequacy_calculations",
                        to="reference_model.DERSimulation",
                    ),
                ),
                (
                    "system_profile",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="stored_resource_adequacy_calculations",
                        to="procurement.SystemProfile",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {("der_simulation", "system_profile")},
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
    ]
