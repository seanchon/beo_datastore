# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin


from reference.reference_model.models import LoadServingEntity


@admin.register(LoadServingEntity)
class LoadServingEntityAdmin(admin.ModelAdmin):
    pass
