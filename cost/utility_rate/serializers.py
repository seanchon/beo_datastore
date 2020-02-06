from rest_framework import serializers

from cost.utility_rate.models import RateCollection, RatePlan


class RatePlanSerializer(serializers.ModelSerializer):
    load_serving_entity = serializers.CharField(
        source="load_serving_entity.name"
    )
    sector = serializers.CharField(source="sector.name")
    voltage_category = serializers.CharField(
        source="voltage_category.name", allow_null=True
    )

    class Meta:
        model = RatePlan
        fields = (
            "load_serving_entity",
            "sector",
            "voltage_category",
            "name",
            "description",
            "demand_min",
            "demand_max",
            "rate_collections",
        )


class RateCollectionSerializer(serializers.ModelSerializer):
    rate_plan_name = serializers.CharField(source="rate_plan.name")

    class Meta:
        model = RateCollection
        fields = (
            "rate_plan_name",
            "rate_plan",
            "rate_data",
            "openei_url",
            "utility_url",
            "effective_date",
        )
