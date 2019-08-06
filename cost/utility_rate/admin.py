# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.utility_rate.models import (
    BillComparison,
    RateCollection,
    RatePlan,
    StoredBillCalculation,
)


class RateCollectionInline(admin.TabularInline):
    model = RateCollection


@admin.register(RatePlan)
class RatePlanAdmin(admin.ModelAdmin):
    inlines = [RateCollectionInline]


@admin.register(RateCollection)
class RateCollectionAdmin(admin.ModelAdmin):
    readonly_fields = [
        "energy_weekday_rates_html_table",
        "energy_weekend_rates_html_table",
        "demand_weekday_rates_html_table",
        "demand_weekend_rates_html_table",
    ]


class BillComparisonInline(admin.TabularInline):
    model = BillComparison


@admin.register(StoredBillCalculation)
class StoredBillCalculation(admin.ModelAdmin):
    inlines = [BillComparisonInline]


@admin.register(BillComparison)
class BillComparisonAdmin(admin.ModelAdmin):
    readonly_fields = ["pre_DER_bill_html", "post_DER_bill_html"]
