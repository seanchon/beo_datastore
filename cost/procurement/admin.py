# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.procurement.models import SystemProfile


@admin.register(SystemProfile)
class SystemProfile(admin.ModelAdmin):
    readonly_fields = [
        "intervalframe_html_plot",
        "average_vs_maximum_html_plot",
    ]
