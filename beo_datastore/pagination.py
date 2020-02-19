from rest_framework.pagination import PageNumberPagination


class DefaultResultsSetPagination(PageNumberPagination):
    """
    The default pagination class. This is required to override the default pagination config that
    comes out of the box with the `PageNumberPagination` class
    """

    max_page_size = 100
    page_size = 20
    page_size_query_param = "page_size"
