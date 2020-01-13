# Generated by Django 2.2.7 on 2019-11-15 00:42

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("optimization", "0002_auto_20191111_2359")]

    operations = [
        migrations.RenameField(
            model_name="simulationoptimization",
            old_name="meters",
            new_name="meter_intervalframes",
        ),
        migrations.AlterField(
            model_name="simulationoptimization",
            name="meter_intervalframes",
            field=models.ManyToManyField(
                blank=True,
                related_name="simulation_optimizations",
                to="reference_model.MeterIntervalFrame",
            ),
        ),
    ]