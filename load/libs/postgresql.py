import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PostgreSQL(object):
    def __init__(
        self, user, password, dbname, host="localhost", *args, **kwargs
    ):
        self.connect(host=host, user=user, password=password, dbname=dbname)

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        self.disconnect()

    def connect(self, user, password, dbname, host="localhost"):
        """
        Connect PostgreSQL connection and cursor.
        """
        self.connection = psycopg2.connect(
            host=host, user=user, password=password, dbname=dbname
        )
        self.cursor = self.connection.cursor()

    def disconnect(self):
        """
        Disconnect/Close PostgreSQL connection.
        """
        self.cursor.close()
        self.connection.close()

    def execute(self, command):
        """
        Executes a database command.

        If database command fails to execute, errors are logged instead of the
        program crashing.
        """
        self.cursor.execute(command)
        self.connection.commit()
        try:
            return self.cursor.fetchall()
        except psycopg2.ProgrammingError:
            return []

    def quote(self, input):
        """
        Returns quoted input for strings.

        Integers and floats are returned as non-quoted vaues.
        """
        if type(input) == int or type(input) == float:
            return input
        else:
            return "'{}'".format(input)

    @classmethod
    def execute_global_command(cls, user, password, command, host="localhost"):
        """
        Execute commands such as CREATE DATABASE and DROP DATABASE. There must
        be a database named <user>, which the user owns.
        """
        with cls(
            host=host, user=user, password=password, dbname="postgres"
        ) as postgres:
            postgres.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return postgres.execute(command)


def format_bulk_insert(chunk):
    """
    Converts an array of strings (comma separated) and returns a string
    formatted for a bulk insert.

    Example:
        INSERT INTO films (code, title, date_prod, kind) VALUES
            ('B6717', 'Tampopo', '1985-02-10', 'Comedy'),
            ('HG120', 'The Dinner Game', '1985-02-10', 'Comedy');

    chunk = [
        ["B6717", "Tampopo", "1985-02-10", "Comedy"],
        ["HG120", "The Dinner Game", "1985-02-10", "Comedy"]
    ]

    Returns:
        "('B6717', 'Tampopo', '1985-02-10', 'Comedy')," +
        "('HG120', 'The Dinner Game', '1985-02-10', 'Comedy')"

    NOTE: Single quote marks are removed.

    :param chunk: array of strings (comma separated)
    """
    return ",".join(
        [
            "("
            + ",".join(["'{}'".format(x.replace("'", "")) for x in line])
            + ")"
            for line in chunk
        ]
    )
