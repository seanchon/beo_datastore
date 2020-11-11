# Generated by Django 2.2.7 on 2020-10-21 18:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0010_auto_20201014_2237"),
        ("ghg", "0003_storedghgcalculation_stacked"),
    ]

    operations = [
        migrations.AlterField(
            model_name="storedghgcalculation",
            name="stacked",
            field=models.BooleanField(default=True),
        ),
        migrations.AlterUniqueTogether(
            name="storedghgcalculation",
            unique_together={("der_simulation", "ghg_rate", "stacked")},
        ),
    ]