# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin


from load.customer.models import ServiceDrop, Meter


class MeterInline(admin.TabularInline):
    model = Meter


@admin.register(ServiceDrop)
class ServiceDropAdmin(admin.ModelAdmin):
    inlines = [MeterInline]


@admin.register(Meter)
class MeterAdmin(admin.ModelAdmin):
    pass
