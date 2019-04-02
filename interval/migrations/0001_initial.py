# Generated by Django 2.1.7 on 2019-03-27 22:11

from django.db import migrations, models
import django.db.models.deletion
import localflavor.us.models


class Migration(migrations.Migration):

    initial = True

    dependencies = [("reference_unit", "0001_initial")]

    operations = [
        migrations.CreateModel(
            name="Meter",
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
                ("export", models.BooleanField(default=False)),
                (
                    "data_unit",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="meters",
                        to="reference_unit.DataUnit",
                    ),
                ),
            ],
            options={"abstract": False},
        ),
        migrations.CreateModel(
            name="ServiceDrop",
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
                ("sa_id", models.IntegerField(db_index=True)),
                ("rate_plan", models.CharField(db_index=True, max_length=16)),
                ("state", localflavor.us.models.USStateField(max_length=2)),
            ],
            options={"abstract": False},
        ),
        migrations.AddField(
            model_name="meter",
            name="service_drop",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="meters",
                to="interval.ServiceDrop",
            ),
        ),
    ]
