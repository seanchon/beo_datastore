# Generated by Django 2.2.7 on 2020-01-25 00:15

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0004_auto_20200125_0015"),
        ("customer", "0004_auto_20191222_2227"),
    ]

    operations = [
        migrations.RemoveField(model_name="meter", name="load_serving_entity"),
        migrations.AddField(
            model_name="meter",
            name="_load_serving_entity",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="meters",
                to="reference_model.LoadServingEntity",
            ),
        ),
    ]