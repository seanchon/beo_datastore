# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin


from load.customer.models import Meter, Channel


class ChannelInline(admin.TabularInline):
    model = Channel


@admin.register(Meter)
class MeterAdmin(admin.ModelAdmin):
    inlines = [ChannelInline]


@admin.register(Channel)
class ChannelAdmin(admin.ModelAdmin):
    pass
