# Generated by Django 2.2.7 on 2020-07-24 17:24

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("customer", "0004_auto_20200407_1706")]

    operations = [
        migrations.AlterUniqueTogether(
            name="customermeter", unique_together=set()
        ),
        migrations.RemoveField(model_name="customermeter", name="export_hash"),
        migrations.RemoveField(model_name="customermeter", name="import_hash"),
    ]
