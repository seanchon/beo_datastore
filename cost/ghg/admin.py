# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.ghg.models import GHGRate, StoredGHGCalculation


@admin.register(GHGRate)
class GHGRateAdmin(admin.ModelAdmin):
    readonly_fields = ["html_table"]
    search_fields = ["id", "name"]
    ordering = ["-effective"]


@admin.register(StoredGHGCalculation)
class StoredGHGCalculationAdmin(admin.ModelAdmin):
    readonly_fields = ["pre_DER_total", "post_DER_total", "net_impact"]
    search_fields = ["id", "battery_simulation__id", "ghg_rate__id"]
