# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from der.simulation.models import (
    BatterySchedule,
    BatteryStrategy,
    BatteryConfiguration,
    StoredBatterySimulation,
)


@admin.register(BatterySchedule)
class BatteryScheduleAdmin(admin.ModelAdmin):
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


@admin.register(StoredBatterySimulation)
class StoredBatterySimulation(admin.ModelAdmin):
    readonly_fields = [
        "pre_vs_post_average_288_html_plot",
        "pre_vs_post_maximum_288_html_plot",
        "average_battery_operations_html_plot",
        "average_state_of_charge_html_plot",
    ]
    search_fields = ["id", "meter__sa_id"]
