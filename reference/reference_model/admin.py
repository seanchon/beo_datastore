# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin


from reference.reference_model.models import BuildingType, DataUnit


@admin.register(BuildingType)
class BuildingTypeAdmin(admin.ModelAdmin):
    pass


@admin.register(DataUnit)
class DataUnitAdmin(admin.ModelAdmin):
    pass
