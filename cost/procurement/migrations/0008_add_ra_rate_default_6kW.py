# Generated by Django 2.2.7 on 2020-10-28 23:31

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('auth_user', '0002_auto_20200812_1906'),
        ('procurement', '0007_auto_20201021_1831'),
    ]

    operations = [
        migrations.AddField(
            model_name='systemprofile',
            name='resource_adequacy_rate',
            field=models.FloatField(default=6.0, validators=[django.core.validators.MinValueValidator(0.0)]),
        ),
        migrations.AlterUniqueTogether(
            name='systemprofile',
            unique_together={('name', 'load_serving_entity', 'resource_adequacy_rate')},
        ),
    ]
