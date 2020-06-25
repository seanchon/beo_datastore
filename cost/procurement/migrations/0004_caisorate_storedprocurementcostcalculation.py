# Generated by Django 2.2.7 on 2020-06-17 20:43

from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr
import jsonfield.fields


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0003_derstrategy_objective"),
        ("procurement", "0003_caisoreport"),
    ]

    operations = [
        migrations.CreateModel(
            name="CAISORate",
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
                ("filters", jsonfield.fields.JSONField()),
                (
                    "caiso_report",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="caiso_rates",
                        to="procurement.CAISOReport",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {("filters", "caiso_report")},
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="StoredProcurementCostCalculation",
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
                    "caiso_rate",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="stored_procurement_cost_calculations",
                        to="procurement.CAISORate",
                    ),
                ),
                (
                    "der_simulation",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="stored_procurement_cost_calculations",
                        to="reference_model.DERSimulation",
                    ),
                ),
            ],
            options={
                "ordering": ["id"],
                "unique_together": {("der_simulation", "caiso_rate")},
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
    ]