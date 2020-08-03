# Generated by Django 2.2.7 on 2020-07-24 17:25

from django.db import migrations, models

from load.customer.models import CustomerMeter


def update_hash(apps, schema_editor):
    """
    Update hash values on BatterySchedule handled in clean() method.
    """
    for meter in CustomerMeter.objects.all():
        if meter.import_channel:
            meter.import_hash = meter.import_channel.intervalframe.__hash__()
        if meter.export_channel:
            meter.export_hash = meter.export_channel.intervalframe.__hash__()
        meter.save()


class Migration(migrations.Migration):

    dependencies = [
        ("auth_user", "0001_initial"),
        ("customer", "0005_auto_20200724_1724"),
    ]

    operations = [
        migrations.AddField(
            model_name="customermeter",
            name="export_hash",
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="customermeter",
            name="import_hash",
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AlterUniqueTogether(
            name="customermeter",
            unique_together={
                ("load_serving_entity", "import_hash", "export_hash")
            },
        ),
        migrations.RunPython(
            update_hash, reverse_code=migrations.RunPython.noop
        ),
    ]