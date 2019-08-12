# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.optimization.models import (
    SimulationOptimization,
    MultiScenarioOptimization,
)


@admin.register(SimulationOptimization)
class SimulationOptimizationAdmin(admin.ModelAdmin):
    exclude = ["meters"]
    readonly_fields = ["detailed_report_html_table"]


@admin.register(MultiScenarioOptimization)
class MultiScenarioOptimizationAdmin(admin.ModelAdmin):
    readonly_fields = ["detailed_report_html_table"]
