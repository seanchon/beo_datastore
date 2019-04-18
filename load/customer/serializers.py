from rest_framework import serializers

from load.customer.models import Meter, ServiceDrop


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
            "count_288",
            "total_288",
            "average_288",
            "peak_288",
        )
