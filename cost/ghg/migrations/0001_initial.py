# Generated by Django 2.2 on 2019-04-09 23:13

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CleanNetShort',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('effective', models.DateField()),
            ],
            options={
                'ordering': ['id'],
            },
        ),
    ]
