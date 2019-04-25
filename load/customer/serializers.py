from rest_framework import serializers

from load.customer.models import Channel, Meter


class MeterSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Meter
        fields = ("sa_id", "rate_plan", "state", "channels")


class ChannelSerializer(serializers.HyperlinkedModelSerializer):
    sa_id = serializers.CharField(source="meter.sa_id")
    data_unit = serializers.CharField(source="data_unit.name")

    class Meta:
        model = Channel
        fields = (
            "sa_id",
            "export",
            "data_unit",
            "meter",
            "count_288",
            "total_288",
            "average_288",
            "peak_288",
        )
