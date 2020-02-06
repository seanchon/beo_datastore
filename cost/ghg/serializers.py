from rest_framework import serializers

from cost.ghg.models import GHGRate


class GHGRateSerializer(serializers.ModelSerializer):
    rate_unit = serializers.CharField(source="rate_unit.__str__")

    class Meta:
        model = GHGRate
        fields = ("effective", "rate_unit", "dataframe")
