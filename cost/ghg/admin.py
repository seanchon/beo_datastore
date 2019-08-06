# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.ghg.models import GHGRate, StoredGHGCalculation


@admin.register(GHGRate)
class GHGRateAdmin(admin.ModelAdmin):
    readonly_fields = ["html_table"]


@admin.register(StoredGHGCalculation)
class StoredGHGCalculationAdmin(admin.ModelAdmin):
    readonly_fields = ["net_impact", "comparision_html_table"]
