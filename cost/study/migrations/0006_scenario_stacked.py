# Generated by Django 2.2.7 on 2020-10-21 18:08

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("study", "0005_auto_20201013_2310")]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="stacked",
            field=models.BooleanField(default=True),
        )
    ]
