from django_auto_repr import AutoRepr
from polymorphic.models import PolymorphicModel

from django.db import models, transaction


class ValidationModel(AutoRepr, models.Model):
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


class PolymorphicValidationModel(AutoRepr, PolymorphicModel):
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


class FrameFileMixin(object):
    """
    A collection of methods for use with a Django model utilizing a dataframe
    stored as a parquet file.
    """

    @property
    def frame_file_class(self):
        """
        Required by FrameFileMixin methods. Should be set as an attribute
        pointing to DataFrameFile class.

        ex.
            frame_file_class = GHGRateFrame288
        """
        raise NotImplementedError("frame_file_class property must be set.")

    @property
    def frame(self):
        """
        Retrieves frame from parquet file.
        """
        if not hasattr(self, "_frame"):
            self._frame = self.frame_file_class.get_frame_from_file(
                reference_object=self
            )
        return self._frame

    @frame.setter
    def frame(self, frame):
        """
        Sets frame property. Writes to disk on save().
        """
        self._frame = frame

    def save(self, *args, **kwargs):
        if hasattr(self, "_frame"):
            self._frame.save()
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        if hasattr(self, "_frame"):
            self._frame.delete()
        super().delete(*args, **kwargs)

    @classmethod
    def create(cls, dataframe, *args, **kwargs):
        """
        Create instance of cls with an attached dataframe.
        """
        with transaction.atomic():
            reference_object = cls.objects.create(*args, **kwargs)
            reference_object.frame = cls.frame_file_class(
                reference_object=reference_object, dataframe=dataframe
            )
            reference_object.save()

            return reference_object

    @classmethod
    def get_or_create(cls, dataframe, *args, **kwargs):
        """
        Fetch existing or create instance of cls with an attached dataframe.
        """
        objects = cls.objects.filter(*args, **kwargs)
        if objects:
            return (objects.first(), False)
        else:
            return (cls.create(dataframe, *args, **kwargs), True)


class Frame288FileMixin(FrameFileMixin):
    """
    A collection of methods for use with a Django model utilizing a
    Frame288File stored as a parquet file.
    """

    @property
    def frame288(self):
        return self.frame

    @frame288.setter
    def frame288(self, frame288):
        self.frame = frame288


class IntervalFrameFileMixin(FrameFileMixin):
    """
    A collection of methods for use with a Django model utilizing a
    IntervalFrameFile stored as a parquet file.
    """

    @property
    def intervalframe(self):
        return self.frame

    @intervalframe.setter
    def intervalframe(self, intervalframe):
        self.frame = intervalframe
