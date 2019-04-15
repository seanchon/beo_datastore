# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from load.openei.models import ReferenceBuilding


@admin.register(ReferenceBuilding)
class ReferenceBuildingAdmin(admin.ModelAdmin):
    pass
