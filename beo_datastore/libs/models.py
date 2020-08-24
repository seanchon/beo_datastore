from functools import reduce

from polymorphic.models import PolymorphicModel

from django.apps import apps
from django.db import models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.views import dataframe_to_html


def get_exact_many_to_many(model, m2m_field, ids):
    """
    Return a model QuerySet where there is an exact match on ids using the
    m2m_field.

    :param model: Django model
    :param m2m_field: string
    :param ids: list of ids
    :return: model QuerySet
    """
    initial_qs = model.objects.annotate(cnt=models.Count(m2m_field)).filter(
        cnt=len(ids)
    )
    return reduce(lambda qs, pk: qs.filter(**{m2m_field: pk}), ids, initial_qs)


def get_model_from_any_app(model_name, parent_class=None):
    """
    Return the first Django model found that matches a string. If parent_class
    is passed, the results are limited to the parent_class or its childrens.

    Caution: The same model name can exist across different applications.

    :param model_name: string
    :param parent_class: Django model
    :return: Django model
    """
    for app_config in apps.get_app_configs():
        try:
            model = app_config.get_model(model_name)
            if parent_class and (
                model != parent_class
                and model not in parent_class.__subclasses__()
            ):
                return None
            else:
                return model
        except LookupError:
            pass
    return None


class AutoReprMixin(object):
    """
    A model mixin that generates a useful __repr__
    """

    # fields to exclude from __repr__
    repr_exclude_fields = []

    def _repr_format_field(self, field):
        """
        Get a "bar='bar_value'" str.
        field is a Django Field object.
        """
        if isinstance(field, models.ForeignKey):
            field_name = field.name + "_id"
        else:
            field_name = field.name

        field_value = getattr(self, field_name)

        default = field.default
        if field_value == default:
            return ""
        elif default is models.NOT_PROVIDED:
            if isinstance(field, models.CharField) and field_value == "":
                return ""
            if field_value is None:
                return ""

        return "{}={!r}".format(field_name, field_value)

    def __repr__(self):
        fields = [
            x
            for x in self.__class__._meta.fields
            if x.name not in self.repr_exclude_fields
        ]
        parts = filter(None, map(self._repr_format_field, fields))
        attrs = ", ".join(parts)
        return "{}({})".format(self.__class__.__name__, attrs)


class ValidationModel(AutoReprMixin, models.Model):
    """
    A custom implementation of Django's default Model, which runs data
    validations prior to saving. Models which inherit from ValidationModel
    should implement additional validation checks by overriding the clean()
    method.
    """

    class Meta:
        abstract = True

    def __str__(self):
        return self.__repr__()

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def _reset_cached_properties(self):
        """
        Resets values of cached properties.
        """
        for key, value in self.__class__.__dict__.items():
            if isinstance(value, cached_property):
                self.__dict__.pop(key, None)


class PolymorphicValidationModel(AutoReprMixin, PolymorphicModel):
    """
    A custom implementation of PolymorphicModel, which runs data
    validations prior to saving. Models which inherit from
    PolymorphicValidationModel should implement additional validation checks by
    overriding the clean() method.
    """

    class Meta(PolymorphicModel.Meta):
        abstract = True

    def __str__(self):
        return self.__repr__()

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def _reset_cached_properties(self):
        """
        Resets values of cached properties.
        """
        for key, value in self.__class__.__dict__.items():
            if isinstance(value, cached_property):
                self.__dict__.pop(key, None)

    @property
    def object_type(self):
        """
        String representation of object type.
        """
        return self.polymorphic_ctype.model_class().__name__


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
        raise NotImplementedError(
            "frame_file_class must be set in {}.".format(self.__class__)
        )

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

    @property
    def filename(self):
        return self.frame_file_class.get_filename(reference_object=self)

    @property
    def file_path(self):
        """
        Full file path of parquet file.
        """
        return self.frame_file_class.get_file_path(reference_object=self)

    @property
    def html_table(self):
        """
        Return self.frame as Django-formatted HTML dataframe.
        """
        return dataframe_to_html(self.frame.dataframe)

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
        Custom model.objects.create() method with additional dataframe
        parameter which creates object and associated DataFrameFile.

        :param dataframe: valid pandas DataFrame of self.frame_file_class
        :return: object
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
        Custom model.objects.get_or_create() method with additional dataframe
        parameter which gets or creates object and associated DataFrameFile.

        :param dataframe: valid pandas DataFrame of self.frame_file_class
        :return: (object, object created (True/False))
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
        self.frame = self.frame_file_class(
            dataframe=frame288.dataframe, reference_object=self
        )


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
        self.frame = self.frame_file_class(
            dataframe=intervalframe.dataframe, reference_object=self
        )
