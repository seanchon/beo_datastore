from rest_framework import serializers

from interval.models import Meter, ServiceDrop


class ServiceDropSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = ServiceDrop
        fields = ("sa_id", "rate_plan", "state", "meters")


class MeterSerializer(serializers.HyperlinkedModelSerializer):
    sa_id = serializers.CharField(source="service_drop.sa_id")
    data_unit = serializers.CharField(source="data_unit.name")

    class Meta:
        model = Meter
        fields = (
            "sa_id",
            "export",
            "data_unit",
            "service_drop",
            "average_288_dataframe",
            "maximum_288_dataframe",
            "count_288_dataframe",
        )
