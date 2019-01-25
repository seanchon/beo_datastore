from polymorphic.models import PolymorphicModel

from django.db import models


class ValidationModel(models.Model):
    """
    A custom implementation of Django's default Model, which runs data
    validations prior to saving. Models which inherit from ValidationModel
    should implement additional validation checks by overriding the clean()
    method.
    """
    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    class Meta:
        abstract = True


class PolymorphicValidationModel(PolymorphicModel):
    """
    A custom implementation of PolymorphicModel, which runs data
    validations prior to saving. Models which inherit from
    PolymorphicValidationModel should implement additional validation checks by
    overriding the clean() method.
    """
    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    class Meta(PolymorphicModel.Meta):
        abstract = True
