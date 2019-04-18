# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.utility_rate.models import RateCollection, RatePlan


class RateCollectionInline(admin.TabularInline):
    model = RateCollection


@admin.register(RatePlan)
class RatePlanAdmin(admin.ModelAdmin):
    inlines = [RateCollectionInline]


@admin.register(RateCollection)
class RateCollectionAdmin(admin.ModelAdmin):
    pass
