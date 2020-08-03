# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from der.simulation.models import (
    DERSchedule,
    BatteryStrategy,
    BatteryConfiguration,
    EVSEConfiguration,
    EVSEStrategy,
    StoredBatterySimulation,
)


@admin.register(DERSchedule)
class DERScheduleAdmin(admin.ModelAdmin):
    readonly_fields = ["html_table"]
    search_fields = ["id"]


@admin.register(BatteryStrategy)
class BatteryStrategy(admin.ModelAdmin):
    ordering = ["name"]
    readonly_fields = [
        "charge_discharge_html_plot",
        "charge_schedule_html_table",
        "discharge_schedule_html_table",
    ]
    search_fields = [
        "id",
        "name",
        "charge_schedule__id",
        "discharge_schedule__id",
    ]


@admin.register(BatteryConfiguration)
class BatteryConfiguration(admin.ModelAdmin):
    search_fields = ["rating", "discharge_duration_hours", "efficiency"]


@admin.register(EVSEStrategy)
class EVSEStrategyAdmin(admin.ModelAdmin):
    ordering = ["name"]
    readonly_fields = [
        "charge_drive_html_plot",
        "charge_schedule_html_table",
        "drive_schedule_html_table",
    ]
    search_fields = [
        "id",
        "name",
        "charge_schedule__id",
        "drive_schedule__id",
    ]


@admin.register(EVSEConfiguration)
class EVSEConfigurationAdmin(admin.ModelAdmin):
    search_fields = [
        "ev_mpkwh",
        "ev_mpg_eq",
        "ev_capacity",
        "ev_efficiency",
        "evse_rating",
        "ev_count",
        "evse_count",
    ]


@admin.register(StoredBatterySimulation)
class StoredBatterySimulation(admin.ModelAdmin):
    readonly_fields = [
        "pre_vs_post_average_288_html_plot",
        "pre_vs_post_maximum_288_html_plot",
        "average_battery_operations_html_plot",
        "average_state_of_charge_html_plot",
    ]
    search_fields = ["id", "meter__sa_id"]
