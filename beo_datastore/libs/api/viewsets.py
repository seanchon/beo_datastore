from dynamic_rest.viewsets import WithDynamicViewSetMixin
from rest_framework import mixins
from rest_framework import viewsets

from beo_datastore.libs.api.pagination import DefaultResultsSetPagination


class BaseViewSet(viewsets.GenericViewSet):
    """
    Provides helper methods to view sets
    """

    def _param(self, query_param: str) -> str:
        """
        Shortcut for accessing the request object's query parameters

        :param query_param: name of the query parameter to fetch
        """
        return self.request.query_params.get(query_param)


class CreateViewSet(mixins.CreateModelMixin, BaseViewSet):
    """
    A viewset that only allows POST.
    """

    pass


class ListRetrieveViewSet(
    WithDynamicViewSetMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    BaseViewSet,
):
    """
    A dynamic_rest viewset that only allows GET.
    """

    pagination_class = DefaultResultsSetPagination


class ListRetrieveUpdateDestroyViewSet(
    WithDynamicViewSetMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    mixins.UpdateModelMixin,
    mixins.DestroyModelMixin,
    BaseViewSet,
):
    """
    A dynamic_rest viewset that only allows GET, PATCH, and DELETE.
    """

    pagination_class = DefaultResultsSetPagination

    def get_serializer(self, *args, **kwargs):
        """
        This method is taken from dynamic_rest's `WithDynamicViewSetMixin`
        class. The method in that class modifies the "included fields"
        parameter to include all fields in the responses to PATCH, POST and PUT
        requests, regardless of the `include[]` and `exclude[]` query
        parameters. This is undesirable for our purposes, so we have taken that
        method and removed that behavior.
        """
        if "request_fields" not in kwargs:
            kwargs["request_fields"] = self.get_request_fields()
        if "sideloading" not in kwargs:
            kwargs["sideloading"] = self.get_request_sideloading()
        if "debug" not in kwargs:
            kwargs["debug"] = self.get_request_debug()
        if "envelope" not in kwargs:
            kwargs["envelope"] = True

        # Deliberately skips the parent class's method, which modifies the "included fields"
        return super(WithDynamicViewSetMixin, self).get_serializer(
            *args, **kwargs
        )


class ListRetrieveDestroyViewSet(
    WithDynamicViewSetMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    mixins.DestroyModelMixin,
    BaseViewSet,
):
    """
    A dynamic_rest viewset that only allows GET and DELETE.
    """

    pagination_class = DefaultResultsSetPagination
