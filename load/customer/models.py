from functools import reduce
import os
import pandas.io.sql as sqlio
import re
import us
import uuid

from django.contrib.auth.models import User
from django.db import connection, models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.clustering import KMeansLoadClustering
from beo_datastore.libs.intervalframe import ValidationIntervalFrame
from beo_datastore.libs.intervalframe_file import (
    Frame288File,
    IntervalFrameFile,
)
from beo_datastore.libs.models import (
    ValidationModel,
    Frame288FileMixin,
    IntervalFrameFileMixin,
)
from beo_datastore.libs.plot_intervalframe import (
    plot_frame288,
    plot_intervalframe,
    plot_frame288_monthly_comparison,
)
from beo_datastore.libs.postgresql import PostgreSQL, format_bulk_insert
from beo_datastore.settings import DATABASES, MEDIA_ROOT
from beo_datastore.libs.utils import bytes_to_str, file_md5sum

from reference.reference_model.models import (
    DataUnit,
    LoadServingEntity,
    Meter,
    MeterGroup,
)


class OriginFileIntervalFrame(IntervalFrameFile):
    """
    Model for handling OriginFile IntervalFrameFiles, which have timestamps and
    values.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "origin_files")


class OriginFile(IntervalFrameFileMixin, MeterGroup):
    """
    File containing customer Meter and Channel data.
    """

    file = models.FileField(upload_to="origin_files")
    expected_meter_count = models.IntegerField(blank=True, null=True)
    md5sum = models.CharField(max_length=32)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="origin_files",
        on_delete=models.PROTECT,
        blank=False,
        null=False,
    )
    owners = models.ManyToManyField(
        to=User, related_name="origin_files", blank=True
    )

    # Required by IntervalFrameFileMixin.
    frame_file_class = OriginFileIntervalFrame

    class Meta:
        ordering = ["id"]

    @property
    def meter_intervalframe(self):
        return self.intervalframe

    @property
    def file_path(self):
        try:
            return self.file.path
        except NotImplementedError:  # S3Boto3StorageFile
            return os.path.join(MEDIA_ROOT, str(self.file))

    @cached_property
    def file_header(self):
        """
        Return first line of self.file.
        """
        with self.file.open(mode="r") as f:
            return bytes_to_str(f.readline()).strip()

    @property
    def csv_columns(self):
        """
        CSV columns from self.file. Double-quoted to handle PostgreSQL special
        character constraints.
        """
        return ['"{}"'.format(x) for x in self.file_header.split(",")]

    @property
    def db_exists(self):
        """
        Return True if corresponding database exists.
        """
        return bool(
            self.db_execute_global(
                "SELECT datname FROM pg_catalog.pg_database "
                "WHERE datname = '{}'".format(self.db_name)
            )
        )

    @property
    def db_name(self):
        """
        Name of database containing contents of OriginFile.file.
        """
        return "origin_file_" + str(self.id).replace("-", "_")

    @property
    def db_table(self):
        """
        Table used within database (self.db_name).
        """
        return "intervals"

    @property
    def db_sa_id_column(self):
        """
        Return column containing SA ID.
        """
        sa_columns = [x for x in self.csv_columns if "SA" in x]
        if len(sa_columns) != 1:
            raise LookupError("Unique SA ID column not found.")
        else:
            return sa_columns[0]

    @classmethod
    def get_or_create(cls, file, name, load_serving_entity, owner=None):
        """
        Create OriginFile and assign ownership. If OriginFile already exists,
        only assign ownership.

        :param file: file path
        :param name: string
        :param load_serving_entity: LoadServingEntity
        :param user: Django User object
        :return: (OriginFile, created)
        """
        with transaction.atomic():
            origin_file = OriginFile(
                name=name, load_serving_entity=load_serving_entity, md5sum="0"
            )
            origin_file.file.save(os.path.basename(file.name), file, save=True)
            origin_file.md5sum = file_md5sum(origin_file.file.file)
            origin_file.save()
            if owner:
                origin_file.owners.add(owner)

            # TODO: delete duplicate files
            existing_files = OriginFile.objects.filter(
                load_serving_entity=load_serving_entity,
                md5sum=origin_file.md5sum,
            ).exclude(id=origin_file.id)

            if not existing_files:
                return (origin_file, True)
            else:
                origin_file.delete()
                return (existing_files.first(), False)

    def db_connect(self):
        """
        Return PostgreSQL connection.
        """
        return PostgreSQL(
            host=DATABASES["default"]["HOST"],
            user=DATABASES["default"]["USER"],
            password=DATABASES["default"]["PASSWORD"],
            dbname=self.db_name,
        )

    def db_execute_global(self, command):
        """
        Execute PostgreSQL global command.

        :param command: SQL command
        :return: SQL response
        """
        return PostgreSQL.execute_global_command(
            host=DATABASES["default"]["HOST"],
            user=DATABASES["default"]["USER"],
            password=DATABASES["default"]["PASSWORD"],
            command=command,
        )

    def db_create(self):
        """
        Create a database with the contents of OriginFile.file.
        """
        command = "CREATE DATABASE {};".format(self.db_name)
        self.db_execute_global(command=command)

    def db_create_tables(self):
        """
        Create an intervals table to store self.file data.
        """
        with self.db_connect() as postgres:
            # create corresponding columns
            columns = ",".join(
                ["{}".format(x) + " VARCHAR (16)" for x in self.csv_columns]
            )
            command = "CREATE TABLE {}({})".format(self.db_table, columns)
            postgres.execute(command=command)

    def db_load_intervals(self, chunk_size=1000):
        """
        Load data from self.file into database reading chunk_size lines at a
        time.

        :param chunk_size: number of lines to write to db at once
        """
        with self.db_connect() as postgres, self.file.open(mode="r") as f_in:
            i = 0
            chunk = []
            for line in f_in.readlines()[1:]:  # skip header
                chunk.append(bytes_to_str(line).strip())
                i += 1
                if i == chunk_size:
                    # bulk upload
                    command = "INSERT INTO {} ({}) VALUES {};".format(
                        self.db_table,
                        ",".join(self.csv_columns),
                        format_bulk_insert(chunk),
                    )
                    postgres.execute(command=command)
                    i = 0
                    chunk = []
            # bulk upload final chunk
            if chunk:
                command = "INSERT INTO {} ({}) VALUES {};".format(
                    self.db_table,
                    ",".join(self.csv_columns),
                    format_bulk_insert(chunk),
                )
                postgres.execute(command=command)

    def db_create_indexes(self):
        """
        Creates indexes on database. Should be run after data is loaded for
        performance.
        """
        with self.db_connect() as postgres:
            for column in self.csv_columns:
                if "SA" in column or "DATE" in column:
                    command = "CREATE INDEX idx_{} ON {}({});".format(
                        column.replace('"', ""), self.db_table, column
                    )
                    postgres.execute(command=command)

    def db_drop(self):
        """
        Drop database associated with OriginFile.file.
        """
        command = "DROP DATABASE {};".format(self.db_name)
        self.db_execute_global(command=command)

    def db_get_sa_ids(self):
        """
        Return meter SA IDs from database.
        """
        with self.db_connect() as postgres:
            command = (
                "SELECT DISTINCT {} from intervals "
                "ORDER BY {};".format(
                    self.db_sa_id_column, self.db_sa_id_column
                )
            )
            return [x[0] for x in postgres.execute(command=command)]

    def db_get_meter_dataframe(self, sa_ids):
        """
        Return meter dataframe from database where SA ID in sa_ids.

        :param sa_id: SA ID
        :param stack: if True, reformat in ValidationIntervalFrame format
        :return: (forward channel dataframe, reverse channel dataframe)
        """
        if sa_ids:
            sa_ids_str = "({})".format(
                ",".join(["'{}'".format(x) for x in sa_ids])
            )
        else:
            sa_ids_str = "('')"

        with self.db_connect() as postgres:
            command = (
                "SELECT * FROM {} WHERE {} IN {} "
                'ORDER BY "DATE";'.format(
                    self.db_table, self.db_sa_id_column, sa_ids_str
                )
            )
            return sqlio.read_sql_query(sql=command, con=postgres.connection)

    def db_get_meter_group_dataframe(self):
        """
        Run an in-database aggregation of all meter readings grouped by DATE.
        The returned dataframe is the equivalent of having a single aggregate
        meter reading on all meters contained within the file.
        """
        with self.db_connect() as postgres:
            # cleanse non-numeric data using cast, coalesce, substring
            # aggregate data using sum ... GROUP BY
            time_cols = ", ".join(
                [
                    "sum (cast(coalesce(substring({} FROM '^[0-9\.]+$'),'0.0') as float)) "
                    "{}".format(x, x)
                    for x in self.csv_columns
                    if re.search(r"\d", x)
                ]
            )
            command = (
                'SELECT "DATE", "UOM", "DIR", {} FROM intervals '
                'GROUP BY "DATE", "UOM", "DIR" '
                'ORDER BY "DATE";'.format(time_cols)
            )
            return sqlio.read_sql_query(sql=command, con=postgres.connection)


class CustomerMeter(Meter):
    """
    A CustomerMeter is a connection point to the Utility's distribution grid
    identified by a Service Address Identifier (sa_id).
    """

    # TODO: change sa_id to CharField for flexibility
    sa_id = models.BigIntegerField(db_index=True)
    rate_plan_name = models.CharField(
        max_length=64, db_index=True, blank=True, null=True
    )
    multiple_rate_plans = models.BooleanField(default=False)
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="meters",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )
    import_hash = models.CharField(max_length=64, blank=True, null=True)
    export_hash = models.CharField(max_length=64, blank=True, null=True)

    class Meta:
        ordering = ["id"]
        unique_together = ("import_hash", "export_hash")

    def __str__(self):
        return "{} ({}: {})".format(
            self.sa_id, self.load_serving_entity, self.rate_plan_name
        )

    @property
    def meter_intervalframe(self):
        return self.intervalframe

    @property
    def state(self):
        return self.load_serving_entity.state

    @property
    def timezone(self):
        return us.states.lookup(self.state).capital_tz

    @property
    def rate_plan_alias(self):
        """
        Heuristics for linking a rate_plan_name to a RatePlan.
        """
        alias = self.rate_plan_name

        # remove H2 or H from beginning
        if alias.startswith("H2"):
            alias = alias[2:]
        elif alias.startswith("H"):
            alias = alias[1:]
        # remove N from ending
        if alias.endswith("N"):
            alias = alias[:-1]
        if alias == "EVA":
            alias = "EV"
        # convert ETOU to TOU
        alias = alias.replace("ETOU", "E-TOU")

        return alias

    @cached_property
    def linked_rate_plans(self):
        """
        Possible rate plans linked to meter.
        """
        if connection.vendor == "sqlite":
            regex = r"\b{}\b".format(self.rate_plan_alias)
        elif connection.vendor == "postgresql":
            regex = r"\y{}\y".format(self.rate_plan_alias)
        else:
            regex = r""

        return self.load_serving_entity.rate_plans.filter(
            models.Q(name__contains=self.rate_plan_name)
            | models.Q(name__iregex=regex)
        ).distinct()

    @cached_property
    def linked_rate_plan(self):
        """
        Return unique rate plan if one exists.
        """
        if self.linked_rate_plans.count() == 1:
            return self.linked_rate_plans.first()
        else:
            return self.linked_rate_plans.none()

    @property
    def intervalframe(self):
        """
        Return the sum of the import and export channel intervalframes.

        :return: ValidationIntervalFrame
        """
        return reduce(
            lambda a, b: a + b,
            [x.intervalframe for x in self.channels.all()],
            ValidationIntervalFrame(ValidationIntervalFrame.default_dataframe),
        )

    @property
    def import_channel(self):
        try:
            return self.channels.get(export=False)
        except Channel.DoesNotExist:
            return None

    @property
    def export_channel(self):
        try:
            return self.channels.get(export=True)
        except Channel.DoesNotExist:
            return None

    @classmethod
    def get_or_create(
        cls,
        origin_file,
        sa_id,
        rate_plan_name,
        forward_df,
        reverse_df,
        multiple_rate_plans=False,
    ):
        """
        Create a CustomerMeter with an import Channel and export Channel.

        :param origin_file: OriginFile
        :param sa_id: SA ID
        :param rate_plan_name: string
        :param forward_df: import Channel dataframe
        :param reverse_df: export Channel dataframe
        :param multiple_rate_plans: bool
        """
        # get dataframe hash values
        import_hash = ValidationIntervalFrame(dataframe=forward_df).__hash__()
        export_hash = ValidationIntervalFrame(dataframe=reverse_df).__hash__()

        with transaction.atomic():
            meter, created = CustomerMeter.objects.get_or_create(
                sa_id=sa_id,
                rate_plan_name=rate_plan_name,
                multiple_rate_plans=multiple_rate_plans,
                load_serving_entity=origin_file.load_serving_entity,
                import_hash=import_hash,
                export_hash=export_hash,
            )
            meter.meter_groups.add(origin_file)
            for (export, dataframe) in [
                (False, forward_df),
                (True, reverse_df),
            ]:
                meter.get_or_create_channel(export, dataframe)

            return (meter, created)

    def get_or_create_channel(self, export, dataframe, data_unit_name="kw"):
        """
        Create a Channel (import or export) associated with a Meter.

        :param export: True or False
        :param dataframe: dataframe of kW intervals
        :param data_unit_name: "kw" or "kwh"
        """
        if not dataframe.empty:
            Channel.get_or_create(
                export=export,
                data_unit=DataUnit.objects.get(name=data_unit_name),
                meter=self,
                dataframe=dataframe,
            )
        else:
            Channel.get_or_create(
                export=export,
                data_unit=DataUnit.objects.get(name=data_unit_name),
                meter=self,
                dataframe=ValidationIntervalFrame.default_dataframe,
            )


class ChannelIntervalFrame(IntervalFrameFile):
    """
    Model for handling Channel IntervalFrameFiles, which have timestamps and
    values.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "meters")


class Channel(IntervalFrameFileMixin, ValidationModel):
    """
    A Channel is a component of a Meter that tracks energy imported from
    (export=False) or energy exported to (export=True) the grid.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    export = models.BooleanField(default=False)
    data_unit = models.ForeignKey(
        to=DataUnit, related_name="channels", on_delete=models.PROTECT
    )
    meter = models.ForeignKey(
        to=CustomerMeter, related_name="channels", on_delete=models.CASCADE
    )

    # Required by IntervalFrameFileMixin.
    frame_file_class = ChannelIntervalFrame

    class Meta:
        ordering = ["id"]
        unique_together = ("export", "meter")

    def __str__(self):
        return "{} (export: {})".format(self.meter, self.export)

    @property
    def intervalframe_html_plot(self):
        """
        Return Django-formatted HTML average 288 plt.
        """
        return plot_intervalframe(
            intervalframe=self.intervalframe, y_label="kW", to_html=True
        )

    @property
    def average_vs_maximum_html_plot(self):
        """
        Return Django-formatted HTML average vs maximum 288 plt.
        """
        return plot_frame288_monthly_comparison(
            original_frame288=self.intervalframe.average_frame288,
            modified_frame288=self.intervalframe.maximum_frame288,
            to_html=True,
        )

    @property
    def total_288(self):
        """
        Return a 12 x 24 dataframe of totals (sums).
        """
        return self.intervalframe.total_frame288.dataframe

    @property
    def average_288(self):
        """
        Return a 12 x 24 dataframe of averages.
        """
        return self.intervalframe.average_frame288.dataframe

    @property
    def peak_288(self):
        """
        Return a 12 x 24 dataframe of peaks. Export meters return minimum
        values.
        """
        if self.export:
            return self.intervalframe.minimum_frame288.dataframe
        else:
            return self.intervalframe.maximum_frame288.dataframe

    @property
    def count_288(self):
        """
        Return a 12 x 24 dataframe of counts.
        """
        return self.intervalframe.count_frame288.dataframe


class CustomerPopulation(ValidationModel):
    """
    A CustomerPopulation begins with a starting population of customers and
    based on k-means clustering, breaks the populatin of customer into a
    pre-defined number of CustomerClusters.
    """

    name = models.CharField(max_length=128)
    FRAME288_TYPES = (
        ("average_frame288", "average_frame288"),
        ("maximum_frame288", "maximum_frame288"),
        ("minimum_frame288", "minimum_frame288"),
        ("total_frame288", "total_frame288"),
    )
    frame288_type = models.CharField(max_length=16, choices=FRAME288_TYPES)
    normalize = models.BooleanField()
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity,
        related_name="customer_populations",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )

    class Meta:
        ordering = ["id"]
        unique_together = [
            "name",
            "frame288_type",
            "normalize",
            "load_serving_entity",
        ]

    @property
    def number_of_clusters(self):
        """
        Number of associated CustomerCluster objects.
        """
        return self.customer_clusters.count()

    @property
    def meters(self):
        """
        QuerySet of all Meter objects in associated
        CustomerCluster objects.
        """
        return reduce(
            lambda x, y: x | y,
            [x.meters.all() for x in self.customer_clusters.all()],
            Meter.objects.none(),
        )

    @property
    def meter_count(self):
        """
        Number of associated Meter objects.
        """
        return self.meters.count()

    @classmethod
    def generate(
        cls,
        name,
        meters,
        frame288_type,
        number_of_clusters,
        normalize,
        load_serving_entity=None,
    ):
        """
        Create a CustomerPopulation and related CustomerClusters.

        :param name: name of CustomerPopulation
        :param meters: Meter QuerySet
        :param frame288_type: choice - "average_frame288", "minimum_frame288",
            "maximum_frame288", "total_frame288", "count_frame288"
        :param number_of_clusters: number of clusters to create
        :param normalize: True to normalize all ValidationFrame288s to create
            values ranging between -1 and 1
        :param load_serving_entity: LoadServingEntity
        :return CustomerPopulation:
        """
        # return exising CustomerPopulation with same meters
        existing_populations = cls.objects.filter(
            frame288_type=frame288_type,
            normalize=normalize,
            customer_clusters__meters__in=meters,
        ).distinct()
        if existing_populations:
            return existing_populations.first()

        clustering = KMeansLoadClustering(
            objects=meters,
            frame288_type=frame288_type,
            number_of_clusters=number_of_clusters,
            normalize=normalize,
        )

        population = cls.objects.create(
            name=name,
            frame288_type=frame288_type,
            normalize=normalize,
            load_serving_entity=load_serving_entity,
        )

        for i in sorted(set(clustering.cluster_labels)):
            if len(clustering.get_objects_by_cluster_id(i)) == 0:
                # don't create empty clusters
                continue
            cluster = CustomerCluster.create(
                cluster_id=i + 1,
                customer_population=population,
                dataframe=clustering.get_reference_frame288_by_cluster_id(
                    i
                ).dataframe,
            )
            cluster.meters.add(*clustering.get_objects_by_cluster_id(i))

        return population


class CustomerClusterFrame288(Frame288File):
    """
    Model for handling CustomerCluster Frame288Files.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "customer_clusters")


class CustomerCluster(Frame288FileMixin, ValidationModel):
    """
    A CustomerCluster is a sub-population of a CustomerPopulation grouped by
    similar load profiles.
    """

    cluster_id = models.IntegerField()
    customer_population = models.ForeignKey(
        to=CustomerPopulation,
        related_name="customer_clusters",
        on_delete=models.CASCADE,
    )
    meters = models.ManyToManyField(to=Meter, related_name="customer_clusters")

    # Required by Frame288FileMixin.
    frame_file_class = CustomerClusterFrame288

    class Meta:
        ordering = ["id"]

    def __str__(self):
        normalized = " normalize" if self.customer_population.normalize else ""
        return "{} {}{} ({} of {}, ID: {})".format(
            self.customer_population.name,
            self.customer_population.frame288_type,
            normalized,
            self.cluster_id,
            self.customer_population.number_of_clusters,
            self.id,
        )

    @property
    def meter_count(self):
        return self.meters.count()

    @property
    def frame288_html_plot(self):
        """
        Return Django-formatted HTML frame288 plt.
        """
        return plot_frame288(frame288=self.frame288, to_html=True)
