from dynamic_rest.viewsets import WithDynamicViewSetMixin
from rest_framework import mixins
from rest_framework import viewsets

from beo_datastore.libs.api.pagination import DefaultResultsSetPagination


class CreateViewSet(mixins.CreateModelMixin, viewsets.GenericViewSet):
    """
    A viewset that only allows POST.
    """

    pass


class ListRetrieveViewSet(
    WithDynamicViewSetMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    viewsets.GenericViewSet,
):
    """
    A dynamic_rest viewset that only allows GET.
    """

    pagination_class = DefaultResultsSetPagination


class ListRetrieveDestroyViewSet(
    WithDynamicViewSetMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    mixins.DestroyModelMixin,
    viewsets.GenericViewSet,
):
    """
    A dynamic_rest viewset that only allows GET and DELETE.
    """

    pagination_class = DefaultResultsSetPagination
