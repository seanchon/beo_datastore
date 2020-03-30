from django.db import connection, reset_queries
import time
import functools


def query_debugger(func):
    """
    Decorator to debug performance of SQL queries. Results are reported in the
    format:

    Function:
    Number of Queries:
    Finished in:
    """

    @functools.wraps(func)
    def inner_func(*args, **kwargs):

        reset_queries()

        start_queries = len(connection.queries)

        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()

        end_queries = len(connection.queries)

        print("Function: {}".format(func.__name__))
        print("Number of Queries: {}".format(end_queries - start_queries))
        print("Finished in: {:.2f}s".format((end - start)))
        return result

    return inner_func
