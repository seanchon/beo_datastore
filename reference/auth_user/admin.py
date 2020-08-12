from django.contrib import admin

from reference.auth_user.models import EmailDomain, LoadServingEntity, Profile


class EmailDomainInline(admin.TabularInline):
    model = EmailDomain
    readonly_fields = ["domain"]


@admin.register(LoadServingEntity)
class LoadServingEntityAdmin(admin.ModelAdmin):
    inlines = [EmailDomainInline]
    search_fields = ["name", "short_name", "state", "email_domains__name"]


@admin.register(EmailDomain)
class EmailDomainAdmin(admin.ModelAdmin):
    search_fields = [
        "domain",
        "load_serving_entity__name",
        "load_serving_entity__short_name",
    ]


@admin.register(Profile)
class ProfileAdmin(admin.ModelAdmin):
    search_fields = ["user__username", "user__email"]
