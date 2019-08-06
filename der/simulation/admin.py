# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from der.simulation.models import (
    BatterySchedule,
    BatteryConfiguration,
    StoredBatterySimulation,
)


@admin.register(BatterySchedule)
class BatteryScheduleAdmin(admin.ModelAdmin):
    readonly_fields = ["html_table"]


@admin.register(BatteryConfiguration)
class BatteryConfiguration(admin.ModelAdmin):
    pass


@admin.register(StoredBatterySimulation)
class StoredBatterySimulation(admin.ModelAdmin):
    readonly_fields = [
        "pre_vs_post_average_288_html_plot",
        "pre_vs_post_maximum_288_html_plot",
    ]
