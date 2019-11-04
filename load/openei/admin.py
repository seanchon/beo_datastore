# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from load.openei.models import ReferenceBuilding


@admin.register(ReferenceBuilding)
class ReferenceBuildingAdmin(admin.ModelAdmin):
    readonly_fields = [
        "location",
        "state",
        "TMY3_id",
        "source_file_url",
        "building_type",
        "data_unit",
        "intervalframe_html_plot",
        "average_vs_maximum_html_plot",
    ]
