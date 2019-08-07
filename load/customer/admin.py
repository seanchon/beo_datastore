# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from der.simulation.models import StoredBatterySimulation
from load.customer.models import Meter, Channel


class ChannelInline(admin.TabularInline):
    model = Channel
    readonly_fields = ["export", "data_unit", "meter"]


class StoredBatterySimulationInline(admin.TabularInline):
    model = StoredBatterySimulation
    readonly_fields = [
        "start",
        "end_limit",
        "battery_configuration",
        "charge_schedule",
        "discharge_schedule",
    ]


@admin.register(Meter)
class MeterAdmin(admin.ModelAdmin):
    readonly_fields = [
        "sa_id",
        "rate_plan_name",
        "load_serving_entity",
        "intervalframe_html_plot",
        "average_vs_maximum_html_plot",
    ]
    inlines = [ChannelInline, StoredBatterySimulationInline]
    search_fields = ["sa_id", "rate_plan_name", "load_serving_entity__name"]


@admin.register(Channel)
class ChannelAdmin(admin.ModelAdmin):
    readonly_fields = [
        "export",
        "data_unit",
        "meter",
        "intervalframe_html_plot",
        "average_vs_maximum_html_plot",
    ]
    search_fields = [
        "meter__sa_id",
        "meter__rate_plan_name",
        "meter__load_serving_entity__name",
    ]
