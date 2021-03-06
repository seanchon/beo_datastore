# Generated by Django 2.2.7 on 2020-04-07 17:06

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("reference_model", "0001_initial"),
        ("customer", "0003_auto_20200320_1735"),
    ]

    operations = [
        migrations.AddField(
            model_name="customerpopulation",
            name="meter_group",
            field=models.ForeignKey(
                default=1,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="customer_populations",
                to="reference_model.MeterGroup",
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="customerpopulation",
            name="number_of_clusters",
            field=models.IntegerField(default=1),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name="customercluster",
            name="cluster_classifier",
            field=models.OneToOneField(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="customer_cluster",
                to="customer.ClusterClassifier",
            ),
        ),
        migrations.AlterUniqueTogether(
            name="customerpopulation",
            unique_together={
                ("name", "frame288_type", "normalize", "meter_group")
            },
        ),
        migrations.RemoveField(
            model_name="customerpopulation", name="load_serving_entity"
        ),
    ]
