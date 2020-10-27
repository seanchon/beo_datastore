# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.procurement.models import (
    CAISORate,
    CAISOReport,
    SystemProfile,
    StoredProcurementCostCalculation,
    StoredResourceAdequacyCalculation,
)


@admin.register(CAISORate)
class CAISORateAdmin(admin.ModelAdmin):
    readonly_fields = ["caiso_report", "filters", "intervalframe_plot"]
    search_fields = ["report_name", "query_params", "year"]


@admin.register(CAISOReport)
class CAISOReportAdmin(admin.ModelAdmin):
    readonly_fields = ["created_at", "report_name", "query_params", "year"]
    search_fields = ["report_name", "query_params", "year"]


@admin.register(StoredProcurementCostCalculation)
class StoredProcurementCostCalculationAdmin(admin.ModelAdmin):
    readonly_fields = ["pre_DER_total", "post_DER_total", "net_impact"]


@admin.register(SystemProfile)
class SystemProfileAdmin(admin.ModelAdmin):
    readonly_fields = [
        "average_frame288_html_plot",
        "maximum_frame288_html_plot",
    ]
    search_fields = ["name"]


@admin.register(StoredResourceAdequacyCalculation)
class StoredResourceAdequacyCalculationAdmin(admin.ModelAdmin):
    readonly_fields = ["pre_DER_total", "post_DER_total", "net_impact"]
    search_fields = ["battery_simulation__id", "system_profile__id"]
