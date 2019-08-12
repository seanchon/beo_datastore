# Generated by Django 2.2.2 on 2019-08-12 17:09

import beo_datastore.libs.models
from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0001_initial"),
        ("customer", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="CustomerPopulation",
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
                ("name", models.CharField(max_length=128)),
                (
                    "frame288_type",
                    models.CharField(
                        choices=[
                            ("average_frame288", "average_frame288"),
                            ("maximum_frame288", "maximum_frame288"),
                            ("minimum_frame288", "minimum_frame288"),
                            ("total_frame288", "total_frame288"),
                        ],
                        max_length=16,
                    ),
                ),
                ("normalize", models.BooleanField()),
                (
                    "load_serving_entity",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="customer_populations",
                        to="reference_model.LoadServingEntity",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {
                    (
                        "name",
                        "frame288_type",
                        "normalize",
                        "load_serving_entity",
                    )
                },
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="CustomerCluster",
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
                ("cluster_id", models.IntegerField()),
                (
                    "customer_population",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="customer_clusters",
                        to="customer.CustomerPopulation",
                    ),
                ),
                (
                    "meters",
                    models.ManyToManyField(
                        related_name="customer_clusters", to="customer.Meter"
                    ),
                ),
            ],
            options={"ordering": ["id"]},
            bases=(
                beo_datastore.libs.models.Frame288FileMixin,
                django_auto_repr.AutoRepr,
                models.Model,
            ),
        ),
    ]
