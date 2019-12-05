# Generated by Django 2.2.7 on 2020-02-07 16:46

import beo_datastore.libs.models
from django.db import migrations, models
import django_auto_repr


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="GHGRate",
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
                ("effective", models.DateField(blank=True, null=True)),
                (
                    "source",
                    models.URLField(blank=True, max_length=128, null=True),
                ),
            ],
            options={"ordering": ["id"]},
            bases=(
                beo_datastore.libs.models.Frame288FileMixin,
                django_auto_repr.AutoRepr,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="StoredGHGCalculation",
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
            ],
            options={"ordering": ["id"]},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
    ]
