from localflavor.us.models import USStateField
from localflavor.us.us_states import STATE_CHOICES
import re

from django.db import models
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
from django.db.models.signals import post_save
from django.dispatch import receiver

from beo_datastore.libs.models import ValidationModel


class LoadServingEntity(ValidationModel):
    """
    Load serving entity (ex. Utility, CCA).
    """

    name = models.CharField(max_length=32, unique=True)
    short_name = models.CharField(max_length=8, unique=False)
    state = USStateField(choices=STATE_CHOICES)
    _parent_utility = models.ForeignKey(
        to="LoadServingEntity",
        related_name="load_serving_entities",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )

    class Meta:
        ordering = ["id"]
        verbose_name_plural = "load serving entities"

    def __str__(self):
        return self.name

    @property
    def parent_utility(self):
        if self._parent_utility:
            return self._parent_utility
        else:
            return self

    @parent_utility.setter
    def parent_utility(self, parent_utility):
        self._parent_utility = parent_utility

    @classmethod
    def menu(cls):
        """
        Return a list of IDs and LoadServingEntity names. This menu is used in
        various scripts that require a LoadServingEntity as an input.
        """
        return "\n".join(
            [
                "ID: {} NAME: {}".format(x[0], x[1])
                for x in cls.objects.values_list("id", "name")
            ]
        )


class EmailDomain(ValidationModel):
    """
    Email Domain for LoadServingEntity.
    """

    domain = models.CharField(max_length=32, unique=True)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="email_domains",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]

    def clean(self, *args, **kwargs):
        """
        Only allow email domain of the format "@domain.extension".
        """
        regex = r"@[^@]+\.[^@]+"
        if not re.match(regex, self.domain):
            raise ValidationError(
                "EmailDomain must be in format @domain.extension."
            )

        super().clean(*args, **kwargs)


class Profile(ValidationModel):
    """
    Extended User Profile to allow association to LoadServingEntity.
    """

    user = models.OneToOneField(User, on_delete=models.CASCADE)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="profiles",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )

    def __str__(self):
        return self.user.username


@receiver(post_save, sender=User)
def create_user_profile(sender, instance, **kwargs):
    """
    If User has no profile:
        - create Profile.
        - assign related LoadServingEntity based on email address.
    """
    if not hasattr(instance, "profile"):
        profile = Profile.objects.create(user=instance)
        domain = "@" + instance.email.split("@")[-1]
        for email_domain in EmailDomain.objects.filter(domain=domain):
            profile.load_serving_entity = email_domain.load_serving_entity
            profile.save()

    instance.profile.save()
