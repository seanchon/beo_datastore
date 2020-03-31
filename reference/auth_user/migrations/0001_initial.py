# Generated by Django 2.2.7 on 2020-03-19 17:47

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr
import localflavor.us.models


class Migration(migrations.Migration):

    initial = True

    dependencies = [migrations.swappable_dependency(settings.AUTH_USER_MODEL)]

    operations = [
        migrations.CreateModel(
            name="LoadServingEntity",
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
                ("name", models.CharField(max_length=32, unique=True)),
                ("short_name", models.CharField(max_length=8)),
                ("state", localflavor.us.models.USStateField(max_length=2)),
                (
                    "_parent_utility",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="load_serving_entities",
                        to="auth_user.LoadServingEntity",
                    ),
                ),
            ],
            options={
                "verbose_name_plural": "load serving entities",
                "ordering": ["id"],
            },
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="Profile",
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
                (
                    "load_serving_entity",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="profiles",
                        to="auth_user.LoadServingEntity",
                    ),
                ),
                (
                    "user",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={"abstract": False},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="EmailDomain",
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
                ("domain", models.CharField(max_length=32, unique=True)),
                (
                    "load_serving_entity",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="email_domains",
                        to="auth_user.LoadServingEntity",
                    ),
                ),
            ],
            options={"ordering": ["id"]},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
    ]