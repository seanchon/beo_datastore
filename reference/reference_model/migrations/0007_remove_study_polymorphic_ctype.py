# Generated by Django 2.2.7 on 2020-09-28 22:57

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0006_auto_20200910_2023"),
    ]

    operations = [
        migrations.RemoveField(model_name="study", name="polymorphic_ctype",),
    ]
