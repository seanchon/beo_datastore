# Generated by Django 2.2.7 on 2020-10-06 18:50

from django.db import migrations, models
import django.db.models.deletion
import jsonfield.fields


class Migration(migrations.Migration):

    replaces = [
        ("study", "0001_initial"),
        ("study", "0002_auto_20200327_2335"),
        ("study", "0003_auto_20200506_1823"),
        ("study", "0004_singlescenariostudy_caiso_rates"),
        ("study", "0005_auto_20200626_2057"),
        ("study", "0006_auto_20200626_2211"),
        ("study", "0007_auto_20200922_0058"),
    ]

    initial = True

    dependencies = [
        ("ghg", "0002_auto_20200319_1747"),
        ("procurement", "0004_caisorate_storedprocurementcostcalculation"),
        ("auth_user", "0001_initial"),
        ("reference_model", "0001_initial"),
        ("procurement", "0002_auto_20200319_1747"),
        ("utility_rate", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="SingleScenarioStudy",
            fields=[
                (
                    "study_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="reference_model.Study",
                    ),
                ),
                ("start", models.DateTimeField()),
                ("end_limit", models.DateTimeField()),
                (
                    "der_configuration",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="single_scenario_studies",
                        to="reference_model.DERConfiguration",
                    ),
                ),
                (
                    "der_strategy",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="single_scenario_studies",
                        to="reference_model.DERStrategy",
                    ),
                ),
                (
                    "ghg_rates",
                    models.ManyToManyField(
                        blank=True,
                        related_name="single_scenario_studies",
                        to="ghg.GHGRate",
                    ),
                ),
                (
                    "load_serving_entity",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="single_scenario_studies",
                        to="auth_user.LoadServingEntity",
                    ),
                ),
                (
                    "meter_group",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="single_scenario_studies",
                        to="reference_model.MeterGroup",
                    ),
                ),
                (
                    "meters",
                    models.ManyToManyField(
                        blank=True,
                        related_name="single_scenario_studies",
                        to="reference_model.Meter",
                    ),
                ),
                (
                    "rate_plan",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="single_scenario_studies",
                        to="utility_rate.RatePlan",
                    ),
                ),
                (
                    "system_profiles",
                    models.ManyToManyField(
                        blank=True,
                        related_name="single_scenario_studies",
                        to="procurement.SystemProfile",
                    ),
                ),
                (
                    "caiso_rates",
                    models.ManyToManyField(
                        blank=True,
                        related_name="single_scenario_studies",
                        to="procurement.CAISORate",
                    ),
                ),
                (
                    "_report",
                    jsonfield.fields.JSONField(
                        blank=True, default={}, null=True
                    ),
                ),
                (
                    "_report_summary",
                    jsonfield.fields.JSONField(
                        blank=True, default={}, null=True
                    ),
                ),
            ],
            options={"ordering": ["id"], "unique_together": set()},
            bases=("reference_model.study",),
        ),
        migrations.CreateModel(
            name="MultipleScenarioStudy",
            fields=[
                (
                    "study_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="reference_model.Study",
                    ),
                ),
                (
                    "single_scenario_studies",
                    models.ManyToManyField(
                        related_name="multiple_scenario_studies",
                        to="study.SingleScenarioStudy",
                    ),
                ),
            ],
            options={"ordering": ["id"]},
            bases=("reference_model.study",),
        ),
    ]