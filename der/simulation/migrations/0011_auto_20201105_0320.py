# Generated by Django 2.2.7 on 2020-11-05 03:20

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("simulation", "0010_auto_20200914_1939"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="batteryconfiguration", unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="batterystrategy", unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="evseconfiguration", unique_together=set(),
        ),
        migrations.AlterUniqueTogether(
            name="evsestrategy", unique_together=set(),
        ),
    ]