# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.study.models import Scenario


@admin.register(Scenario)
class ScenarioAdmin(admin.ModelAdmin):
    exclude = ["meters"]
    readonly_fields = ["report_html_table"]
    search_fields = ["id", "name"]
