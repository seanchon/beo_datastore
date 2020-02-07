from rest_framework import mixins
from rest_framework import viewsets


class CreateViewSet(mixins.CreateModelMixin, viewsets.GenericViewSet):
    """
    A viewset that provides `create` actions.

    To use it, override the class and set the `.queryset` and
    `.serializer_class` attributes.
    """

    pass


class ListRetrieveViewSet(
    mixins.ListModelMixin, mixins.RetrieveModelMixin, viewsets.GenericViewSet
):
    """
    A viewset that provides `list`, and `retrieve` actions.

    To use it, override the class and set the `.queryset` and
    `.serializer_class` attributes.
    """

    pass


class ListRetrieveDestroyViewSet(
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    mixins.DestroyModelMixin,
    viewsets.GenericViewSet,
):
    """
    A viewset that provides `list`, `retrieve`, and `destroy` actions.

    To use it, override the class and set the `.queryset` and
    `.serializer_class` attributes.
    """

    pass
