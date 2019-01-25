# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from reference.openei.models import BuildingType, ReferenceBuilding


# Register your models here.
@admin.register(BuildingType)
class BuidingTypeAdmin(admin.ModelAdmin):
    pass


@admin.register(ReferenceBuilding)
class ReferenceBuildingAdmin(admin.ModelAdmin):
    pass
