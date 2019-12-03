# Generated by Django 2.2.7 on 2019-12-05 17:24

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("reference_model", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="OriginFile",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("uploaded_at", models.DateTimeField(auto_now_add=True)),
                ("file", models.FileField(upload_to="origin_files/")),
                (
                    "owners",
                    models.ManyToManyField(
                        blank=True,
                        related_name="origin_files",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={"ordering": ["id"]},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.AddField(
            model_name="meterintervalframe",
            name="origin_file",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="meter_intervalframes",
                to="reference_model.OriginFile",
            ),
        ),
    ]
