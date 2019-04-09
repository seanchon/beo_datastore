from rest_framework import serializers

from cost.ghg.models import CleanNetShort


class CleanNetShortSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = CleanNetShort
        fields = ("effective", "lookup_table_dataframe")
