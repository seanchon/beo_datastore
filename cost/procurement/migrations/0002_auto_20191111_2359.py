# Generated by Django 2.2.7 on 2019-11-11 23:59

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("reference_model", "0001_initial"),
        ("simulation", "0001_initial"),
        ("procurement", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="systemprofile",
            name="load_serving_entity",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="system_profiles",
                to="reference_model.LoadServingEntity",
            ),
        ),
        migrations.AddField(
            model_name="storedresourceadequacycalculation",
            name="battery_simulation",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="stored_resource_adequacy_calculations",
                to="simulation.StoredBatterySimulation",
            ),
        ),
        migrations.AddField(
            model_name="storedresourceadequacycalculation",
            name="system_profile",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="stored_resource_adequacy_calculations",
                to="procurement.SystemProfile",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="systemprofile",
            unique_together={("name", "load_serving_entity")},
        ),
        migrations.AlterUniqueTogether(
            name="storedresourceadequacycalculation",
            unique_together={("battery_simulation", "system_profile")},
        ),
    ]
