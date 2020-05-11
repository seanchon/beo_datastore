# Generated by Django 2.2.7 on 2020-05-06 18:23

from django.db import migrations, models
import django.db.models.deletion
import django_auto_repr
import uuid


class Migration(migrations.Migration):

    dependencies = [("study", "0002_auto_20200327_2335")]

    operations = [
        migrations.CreateModel(
            name="Report",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={"abstract": False},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.CreateModel(
            name="ReportSummary",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={"abstract": False},
            bases=(django_auto_repr.AutoRepr, models.Model),
        ),
        migrations.AddField(
            model_name="singlescenariostudy",
            name="_report",
            field=models.OneToOneField(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="single_scenario_study",
                to="study.Report",
            ),
        ),
        migrations.AddField(
            model_name="singlescenariostudy",
            name="_report_summary",
            field=models.OneToOneField(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="single_scenario_study",
                to="study.ReportSummary",
            ),
        ),
    ]
