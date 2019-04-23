# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from cost.ghg.models import GHGRate


@admin.register(GHGRate)
class GHGRateAdmin(admin.ModelAdmin):
    pass
