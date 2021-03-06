# Generated by Django 2.2.7 on 2020-10-13 23:10

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("ghg", "0002_auto_20200319_1747"),
        ("procurement", "0005_auto_20200626_2057"),
        ("study", "0004_auto_20201008_2307"),
    ]

    operations = [
        migrations.RemoveField(model_name="scenario", name="caiso_rates",),
        migrations.RemoveField(model_name="scenario", name="ghg_rates",),
        migrations.RemoveField(model_name="scenario", name="system_profiles",),
        migrations.AddField(
            model_name="scenario",
            name="ghg_rate",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="scenarios",
                to="ghg.GHGRate",
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="procurement_rate",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="scenarios",
                to="procurement.CAISORate",
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="system_profile",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="scenarios",
                to="procurement.SystemProfile",
            ),
        ),
        migrations.AlterField(
            model_name="scenario",
            name="rate_plan",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="scenarios",
                to="utility_rate.RatePlan",
            ),
        ),
    ]
