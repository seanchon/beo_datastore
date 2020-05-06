# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.study.models import SingleScenarioStudy, MultipleScenarioStudy


@admin.register(SingleScenarioStudy)
class SingleScenarioStudyAdmin(admin.ModelAdmin):
    exclude = ["meters"]
    readonly_fields = ["report_html_table"]
    search_fields = ["id", "name"]


@admin.register(MultipleScenarioStudy)
class MultipleScenarioStudyAdmin(admin.ModelAdmin):
    readonly_fields = ["report_html_table"]
    search_fields = ["id", "name"]
