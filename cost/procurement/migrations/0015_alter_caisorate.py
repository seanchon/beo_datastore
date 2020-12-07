# Generated by Django 2.2.7 on 2020-12-01 02:15

import django.core.validators
import django.utils.timezone
import jsonfield.fields
from django.db import migrations, models


def set_existing_fields(apps, _):
    """
    Set name, year, and created_at fields from corresponding
    related CAISOReport fields.
    """
    CAISORate = apps.get_model("procurement", "CAISORate")
    for row in CAISORate.objects.all():
        row.name = f"{row.caiso_report.report_name} {row.caiso_report.year}"
        row.created_at = row.caiso_report.created_at
        row.year = row.caiso_report.year
        row.save(update_fields=["name", "created_at", "year"])


class Migration(migrations.Migration):
    dependencies = [
        ("auth_user", "0002_auto_20200812_1906"),
        ("procurement", "0014_remove_systemprofile_uuid"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="caisorate",
            options={"ordering": ["-updated_at"]},
        ),
        migrations.AddField(
            model_name="caisorate",
            name="created_at",
            field=models.DateTimeField(
                auto_now_add=True, default=django.utils.timezone.now
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="caisorate",
            name="load_serving_entity",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                to="auth_user.LoadServingEntity",
            ),
        ),
        migrations.AddField(
            model_name="caisorate",
            name="name",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="caisorate",
            name="updated_at",
            field=models.DateTimeField(auto_now=True),
        ),
        migrations.AddField(
            model_name="caisorate",
            name="year",
            field=models.IntegerField(
                blank=True,
                null=True,
                validators=[
                    django.core.validators.MinValueValidator(2000),
                    django.core.validators.MaxValueValidator(2050),
                ],
            ),
        ),
        migrations.AlterField(
            model_name="caisorate",
            name="caiso_report",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="caiso_rates",
                to="procurement.CAISOReport",
            ),
        ),
        migrations.AlterField(
            model_name="caisorate",
            name="filters",
            field=jsonfield.fields.JSONField(blank=True, null=True),
        ),
        migrations.AlterUniqueTogether(
            name="caisorate",
            unique_together=set(),
        ),
        # Set name and created_at fields.
        migrations.RunPython(
            set_existing_fields, reverse_code=migrations.RunPython.noop
        ),
    ]
