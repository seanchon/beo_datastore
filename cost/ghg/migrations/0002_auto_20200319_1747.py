# Generated by Django 2.2.7 on 2020-03-19 17:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("reference_model", "0001_initial"),
        ("ghg", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="storedghgcalculation",
            name="der_simulation",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="stored_ghg_calculations",
                to="reference_model.DERSimulation",
            ),
        ),
        migrations.AddField(
            model_name="storedghgcalculation",
            name="ghg_rate",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="stored_ghg_calculations",
                to="ghg.GHGRate",
            ),
        ),
        migrations.AddField(
            model_name="ghgrate",
            name="rate_unit",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="ghg_rates",
                to="reference_model.RateUnit",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="storedghgcalculation",
            unique_together={("der_simulation", "ghg_rate")},
        ),
        migrations.AlterUniqueTogether(
            name="ghgrate", unique_together={("name", "effective")}
        ),
    ]
