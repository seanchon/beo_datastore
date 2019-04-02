from rest_framework import serializers

from reference.openei.models import BuildingType, ReferenceBuilding


class BuildingTypeSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = BuildingType
        fields = (
            "name",
            "floor_area",
            "number_of_floors",
            "reference_buildings",
        )


class ReferenceBuildingSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = ReferenceBuilding
        fields = ("location", "state", "source_file_url", "building_type")
