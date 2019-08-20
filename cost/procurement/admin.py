# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.procurement.models import (
    SystemProfile,
    StoredResourceAdequacyCalculation,
)


@admin.register(SystemProfile)
class SystemProfile(admin.ModelAdmin):
    readonly_fields = [
        "intervalframe_html_plot",
        "average_vs_maximum_html_plot",
    ]
    search_fields = ["id", "name"]


@admin.register(StoredResourceAdequacyCalculation)
class StoredResourceAdequacyCalculationAdmin(admin.ModelAdmin):
    readonly_fields = ["net_impact", "comparision_html_table"]
    search_fields = ["id", "battery_simulation__id", "system_profile__id"]
