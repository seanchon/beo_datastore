# Generated by Django 2.2.7 on 2020-03-20 17:35

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("auth_user", "0001_initial"),
        ("customer", "0002_auto_20200319_1747"),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name="customermeter",
            unique_together={
                ("load_serving_entity", "import_hash", "export_hash")
            },
        )
    ]