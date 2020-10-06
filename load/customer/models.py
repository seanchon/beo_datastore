from datetime import datetime
from functools import reduce
import os
import pandas.io.sql as sqlio
import us
import uuid

from django.db import models, transaction
from django.db.models import Count
from django.utils.functional import cached_property

from beo_datastore.libs.clustering import KMeansLoadClustering
from beo_datastore.libs.ingest import (
    csv_split,
    get_timedelta_from_time_strings,
    get_timestamp_columns,
    is_time_str,
    shift_time_string,
    reformat_item_17,
)
from beo_datastore.libs.intervalframe import PowerIntervalFrame
from beo_datastore.libs.intervalframe_file import (
    Frame288File,
    PowerIntervalFrameFile,
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
from beo_datastore.libs.utils import bytes_to_str, file_md5sum
from beo_datastore.settings import DATABASES, MEDIA_ROOT

from reference.reference_model.models import (
    DataUnit,
    DERSimulation,
    Meter,
    MeterGroup,
)
from reference.auth_user.models import LoadServingEntity


class OriginFileIntervalFrame(PowerIntervalFrameFile):
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

    # Required by IntervalFrameFileMixin.
    frame_file_class = OriginFileIntervalFrame

    class Meta:
        ordering = ["id"]

    @property
    def has_completed(self):
        """
        True if all meters have been ingested and self.meter_intervalframe has
        been aggregated.
        """
        return (
            self.expected_meter_count == self.meters.count()
        ) and not self.meter_intervalframe.dataframe.empty

    @property
    def meter_intervalframe(self):
        return self.intervalframe

    @cached_property
    def base_customer_meters(self):
        """
        Base upstream CustomerMeters. This is useful when an OriginFile
        contains DERSimulations since the rate_plan_name is a field on the
        upstream CustomerMeter.
        """
        customer_meter_ids = set(
            self.meters.instance_of(CustomerMeter).values_list("id", flat=True)
        )
        der_simulation_ids = set(
            self.meters.instance_of(DERSimulation).values_list("id", flat=True)
        )

        while der_simulation_ids:
            meter_ids = set(
                DERSimulation.objects.filter(
                    id__in=der_simulation_ids
                ).values_list("meter__id", flat=True)
            )
            upstream_meters = Meter.objects.filter(id__in=meter_ids)
            customer_meter_ids = customer_meter_ids.union(
                set(
                    upstream_meters.instance_of(CustomerMeter).values_list(
                        "id", flat=True
                    )
                )
            )
            der_simulation_ids = set(
                upstream_meters.instance_of(DERSimulation).values_list(
                    "id", flat=True
                )
            )

        return CustomerMeter.objects.filter(id__in=customer_meter_ids)

    @cached_property
    def linked_rate_plan_names(self):
        """
        All rate_plan_name Meter fields linked to OriginFile.
        """
        return set(
            self.base_customer_meters.values_list("rate_plan_name", flat=True)
        )

    @cached_property
    def linked_rate_plan_names_by_frequency(self):
        """
        All rate_plan_name Meter fields linked to OriginFile with frequency
        count.
        """
        return (
            self.base_customer_meters.values("rate_plan_name")
            .order_by()
            .annotate(Count("rate_plan_name"))
        )

    @cached_property
    def primary_linked_rate_plan_name(self):
        """
        Highest-frequency rate_plan_name Meter field linked to OriginFile.
        """
        if self.linked_rate_plan_names_by_frequency:
            return max(
                self.linked_rate_plan_names_by_frequency,
                key=lambda x: x["rate_plan_name__count"],
            )["rate_plan_name"]
        else:
            return ""

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

    @cached_property
    def csv_columns(self):
        """
        CSV columns from self.file.
        """
        return self.file_header.split(",")

    @cached_property
    def csv_sa_id_column(self):
        """
        CSV column containing meter IDs.
        """
        id_columns = [x for x in self.csv_columns if "SA" in x]
        if len(id_columns) != 1:
            raise LookupError("Unique meter ID column not found.")
        else:
            return id_columns[0]

    @property
    def db_sa_id_column(self):
        """
        DB column containing meter IDs. Double-quoted for PostgreSQL.
        """
        return '"SA"'

    @cached_property
    def csv_date_column(self):
        """
        CSV column containing date.
        """
        date_columns = [x for x in self.csv_columns if "date" in x.lower()]
        if len(date_columns) != 1:
            raise LookupError("Unique DATE column not found.")
        else:
            return date_columns[0]

    @property
    def db_date_column(self):
        """
        DB column containing date. Double-quoted for PostgreSQL.
        """
        return '"DATE"'

    @cached_property
    def db_columns(self):
        """
        self.csv_columns using standardized column names. Double-quoted to
        handle PostgreSQL special character constraints.
        """
        db_columns = self.csv_columns

        for i, value in enumerate(db_columns):
            if value == self.csv_sa_id_column:
                db_columns[i] = self.db_sa_id_column
            elif value == self.csv_date_column:
                db_columns[i] = self.db_date_column

        # shift intervals to beginning
        period = get_timedelta_from_time_strings(
            time_strings=get_timestamp_columns(columns=db_columns)
        )
        db_columns = [
            shift_time_string(x, period * -1) if is_time_str(x) else x
            for x in db_columns
        ]

        return ['"{}"'.format(x.replace('"', "")) for x in db_columns]

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

    def aggregate_meter_intervalframe(self) -> bool:
        """
        Only aggretate meter_intervalframe if all self.meters have been
        ingested.
        """
        with self.lock():
            if self.meters.count() == self.expected_meter_count:
                self.intervalframe = reduce(
                    lambda x, y: x + y,
                    (x.meter_intervalframe for x in self.meters.all()),
                    PowerIntervalFrame(),
                )
                self.save()

    @classmethod
    def get_or_create(cls, file, name, load_serving_entity, owner=None):
        """
        Create OriginFile unique on owners, file contents, and LSE.

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

            existing_files = OriginFile.objects.filter(
                load_serving_entity=load_serving_entity,
                md5sum=origin_file.md5sum,
                owners=owner,
            ).exclude(id=origin_file.id)

            if not existing_files:
                created = True
            else:
                origin_file.delete()
                origin_file = existing_files.first()
                created = False

            if owner:
                origin_file.owners.add(owner)

            if owner and not created:
                # rename exlusively owned files on subsequent upload
                for file in existing_files:
                    if file.owners.count() == 1:
                        file.name = name
                        file.save()

            return (origin_file, created)

    @staticmethod
    def db_execute_global(command):
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

    @classmethod
    def db_get_orphaned_db_names(cls):
        """
        Return database names with the "origin_file_" prefix not associated
        with an existing OriginFile.

        :return: set of db names
        """
        db_names = {
            x[0]
            for x in cls.db_execute_global(
                "SELECT datname FROM pg_database "
                "WHERE datistemplate = false AND datname LIKE 'origin_file_%';"
            )
        }

        existing_db_names = {x.db_name for x in cls.objects.all()}
        db_names = db_names - existing_db_names

        return db_names

    @classmethod
    def db_bulk_delete_origin_file_dbs(cls, older_than=datetime.min):
        """
        Bulk delete databases with the "origin_file_" prefix that were created
        before older_than. In addition, deletes any orphaned OriginFile
        databases.

        :param older_than: datetime
        """
        db_names = {
            x.db_name
            for x in OriginFile.objects.filter(created_at__lt=older_than)
        }
        db_names = db_names.union(cls.db_get_orphaned_db_names())

        for db_name in db_names:
            cls.db_execute_global(
                "DROP DATABASE IF EXISTS {};".format(db_name)
            )

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
                ["{}".format(x) + " VARCHAR (128)" for x in self.db_columns]
            )
            command = "CREATE TABLE {}({})".format(self.db_table, columns)
            postgres.execute(command=command)

    def db_load_intervals(self, chunk_size=1000):
        """
        Load data from self.file into database reading chunk_size lines at a
        time.

        :param chunk_size: number of lines to write to db at once
        """
        with self.lock(), self.db_connect() as postgres, self.file.open(
            mode="r"
        ) as f_in:
            i = 0
            chunk = []
            for line in f_in.readlines()[1:]:  # skip header
                if isinstance(line, bytes):  # AWS S3 files
                    line = line.decode()
                chunk.append(csv_split(line))
                i += 1
                if i == chunk_size:
                    # bulk upload
                    command = "INSERT INTO {} ({}) VALUES {};".format(
                        self.db_table,
                        ",".join(self.db_columns),
                        format_bulk_insert(chunk),
                    )
                    postgres.execute(command=command)
                    i = 0
                    chunk = []
            # bulk upload final chunk
            if chunk:
                command = "INSERT INTO {} ({}) VALUES {};".format(
                    self.db_table,
                    ",".join(self.db_columns),
                    format_bulk_insert(chunk),
                )
                postgres.execute(command=command)

    def db_create_indexes(self):
        """
        Creates indexes on database. Should be run after data is loaded for
        performance.
        """
        with self.db_connect() as postgres:
            for column in self.db_columns:
                if (
                    self.db_sa_id_column in column
                    or self.db_date_column in column
                ):
                    command = "CREATE INDEX idx_{} ON {}({});".format(
                        column.replace('"', ""), self.db_table, column
                    )
                    postgres.execute(command=command)

    def db_aggregate_meter_intervalframes(self):
        """
        Create and store an aggregate PowerIntervalFrame representing all
        constituent Meters.
        """
        with self.lock():
            meter_group_df = self.db_get_meter_group_dataframe()
            d_df = reformat_item_17(
                meter_group_df[meter_group_df["DIR"] == "D"]
            )
            r_df = reformat_item_17(
                meter_group_df[meter_group_df["DIR"] == "R"]
            )
            self.intervalframe.dataframe = d_df.add(r_df, fill_value=0)
            self.save()

    def db_drop(self):
        """
        Drop database associated with OriginFile.file.
        """
        if self.db_exists:
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
        :param stack: if True, reformat in PowerIntervalFrame format
        :return: (forward channel dataframe, reverse channel dataframe)
        """
        if sa_ids:
            sa_ids_str = "({})".format(
                ",".join(["'{}'".format(x) for x in sa_ids])
            )
        else:
            sa_ids_str = "('')"

        with self.db_connect() as postgres:
            command = "SELECT * FROM {} WHERE {} IN {} ORDER BY {};".format(
                self.db_table,
                self.db_sa_id_column,
                sa_ids_str,
                self.db_date_column,
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
                    for x in self.db_columns
                    if is_time_str(x)
                ]
            )
            command = (
                'SELECT {date_col}, "UOM", "DIR", {time_cols} FROM intervals '
                'GROUP BY {date_col}, "UOM", "DIR" '
                "ORDER BY {date_col};".format(
                    date_col=self.db_date_column, time_cols=time_cols
                )
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
    import_hash = models.BigIntegerField(blank=True, null=True)
    export_hash = models.BigIntegerField(blank=True, null=True)

    class Meta:
        ordering = ["id"]
        unique_together = (
            "sa_id",
            "rate_plan_name",
            "multiple_rate_plans",
            "load_serving_entity",
            "import_hash",
            "export_hash",
        )

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
    def intervalframe(self):
        """
        Return the sum of the import and export channel intervalframes.

        :return: PowerIntervalFrame
        """
        return reduce(
            lambda a, b: a + b,
            [x.intervalframe for x in self.channels.all()],
            PowerIntervalFrame(),
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
        import_hash = PowerIntervalFrame(dataframe=forward_df).__hash__()
        export_hash = PowerIntervalFrame(dataframe=reverse_df).__hash__()

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

            meter.build_aggregate_metrics()
            return meter, created

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
                dataframe=PowerIntervalFrame.default_dataframe,
            )


class ChannelIntervalFrame(PowerIntervalFrameFile):
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
    based on k-means clustering, breaks the population of customer into a
    pre-defined number of CustomerClusters.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=128)
    FRAME288_TYPES = (
        ("average_frame288", "average_frame288"),
        ("maximum_frame288", "maximum_frame288"),
        ("minimum_frame288", "minimum_frame288"),
        ("total_frame288", "total_frame288"),
    )
    frame288_type = models.CharField(max_length=16, choices=FRAME288_TYPES)
    number_of_clusters = models.IntegerField()
    normalize = models.BooleanField()
    meter_group = models.ForeignKey(
        to=MeterGroup,
        related_name="customer_populations",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ["name", "frame288_type", "normalize", "meter_group"]

    @property
    def cluster_type(self):
        return self.frame288_type.replace("_frame288", "")

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

    @property
    def primary_linked_rate_plan_name(self):
        """
        primary_linked_rate_plan_name associated with related MeterGroup.
        """
        return self.meter_group.primary_linked_rate_plan_name

    def generate(self, owner=None):
        """
        Create related CustomerClusters.

        :param owner: User
        """
        # do not recreate clusters
        if self.customer_clusters.count() > 0:
            return

        # create CustomerCluster objects immediately then update later
        for i in range(1, self.number_of_clusters + 1):
            cluster = CustomerCluster.objects.create(
                name=self.meter_group.name,
                cluster_id=i,
                customer_population=self,
            )
            if owner:
                cluster.owners.add(owner)

        clustering = KMeansLoadClustering(
            objects=self.meter_group.meters.all(),
            frame288_type=self.frame288_type,
            number_of_clusters=self.number_of_clusters,
            normalize=self.normalize,
        )

        for i in sorted(set(clustering.cluster_labels)):
            if len(clustering.get_objects_by_cluster_id(i)) == 0:
                # don't create empty clusters
                continue
            cluster_classifier = ClusterClassifier.create(
                dataframe=clustering.get_reference_frame288_by_cluster_id(
                    i
                ).dataframe
            )
            cluster = CustomerCluster.objects.get(
                name=self.meter_group.name,
                cluster_id=i + 1,
                customer_population=self,
            )
            cluster.cluster_classifier = cluster_classifier
            cluster.meters.add(*clustering.get_objects_by_cluster_id(i))
            cluster.save()


class ClusterClassifierFrame288(Frame288File):
    """
    Model for handling ClusterClassifier Frame288Files.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "customer_clusters")


class ClusterClassifier(Frame288FileMixin, ValidationModel):
    """
    A reference 288 that is used in the k-means clustering algorithm.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # Required by Frame288FileMixin.
    frame_file_class = ClusterClassifierFrame288

    class Meta:
        ordering = ["id"]


class CustomerClusterIntervalFrame(PowerIntervalFrameFile):
    """
    Model for handling CustomerCluster IntervalFrameFiles, which is an
    aggregate of all Meter objects contained within.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "customer_clusters")


class CustomerCluster(IntervalFrameFileMixin, MeterGroup):
    """
    A CustomerCluster is a sub-population of a CustomerPopulation grouped by
    similar load profiles.
    """

    cluster_id = models.IntegerField()
    cluster_classifier = models.OneToOneField(
        to=ClusterClassifier,
        related_name="customer_cluster",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    customer_population = models.ForeignKey(
        to=CustomerPopulation,
        related_name="customer_clusters",
        on_delete=models.CASCADE,
    )
    meters = models.ManyToManyField(to=Meter, related_name="customer_clusters")

    # Required by IntervalFrameFileMixin.
    frame_file_class = CustomerClusterIntervalFrame

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
    def meter_intervalframe(self):
        return self.intervalframe

    @property
    def primary_linked_rate_plan_name(self):
        return self.customer_population.primary_linked_rate_plan_name

    @property
    def number_of_clusters(self):
        return self.customer_population.number_of_clusters

    @property
    def cluster_type(self):
        return self.customer_population.cluster_type

    @property
    def normalize(self):
        return self.customer_population.normalize

    @property
    def frame288_html_plot(self):
        """
        Return Django-formatted HTML frame288 plt.
        """
        return plot_frame288(
            frame288=self.cluster_classifier.frame288, to_html=True
        )
