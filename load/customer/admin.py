# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from der.simulation.models import StoredBatterySimulation
from load.customer.models import (
    Meter,
    Channel,
    CustomerPopulation,
    CustomerCluster,
)


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
    search_fields = [
        "id",
        "sa_id",
        "rate_plan_name",
        "load_serving_entity__name",
    ]


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
        "meter__id",
        "meter__sa_id",
        "meter__rate_plan_name",
        "meter__load_serving_entity__name",
    ]


class CustomerClusterInline(admin.TabularInline):
    model = CustomerCluster
    readonly_fields = [
        "id",
        "cluster_id",
        "number_of_meters",
        "frame288_html_plot",
    ]
    exclude = ["meters"]


@admin.register(CustomerPopulation)
class CustomerPopulationAdmin(admin.ModelAdmin):
    readonly_fields = ["number_of_clusters"]
    inlines = [CustomerClusterInline]
    search_fields = [
        "id",
        "name",
        "frame288_type",
        "load_serving_entity__name",
    ]


@admin.register(CustomerCluster)
class CustomerClusterAdmin(admin.ModelAdmin):
    readonly_fields = ["number_of_meters", "frame288_html_plot"]
    search_fields = [
        "customer_population__id",
        "customer_population__name",
        "customer_population__frame288_type",
        "customer_population__load_serving_entity__name",
    ]
