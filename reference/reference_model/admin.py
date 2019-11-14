# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin


from reference.reference_model.models import (
    LoadServingEntity,
    MeterIntervalFrame,
)


@admin.register(LoadServingEntity)
class LoadServingEntityAdmin(admin.ModelAdmin):
    pass


@admin.register(MeterIntervalFrame)
class MeterIntervalFrameAdmin(admin.ModelAdmin):
    readonly_fields = [
        "intervalframe_html_plot",
        "average_vs_maximum_html_plot",
    ]
