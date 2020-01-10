from functools import reduce
import os
import us

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
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_model.models import (
    DataUnit,
    LoadServingEntity,
    MeterIntervalFrame,
)


class Meter(MeterIntervalFrame):
    """
    A Meter is a connection point to the Utility's distribution grid
    identified by a Service Address Identifier (sa_id).
    """

    sa_id = models.BigIntegerField(db_index=True, unique=True)
    rate_plan_name = models.CharField(
        max_length=64, db_index=True, blank=True, null=True
    )
    load_serving_entity = models.ForeignKey(
        to=LoadServingEntity, related_name="meters", on_delete=models.PROTECT
    )

    class Meta:
        ordering = ["id"]

    def __str__(self):
        return "{} ({}: {})".format(
            self.sa_id, self.load_serving_entity, self.rate_plan_name
        )

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

    @staticmethod
    def ingest_meters(origin_file, utility_name, load_serving_entity):
        """
        Ingest CSV file into a Meter objects.

        :param origin_file: OriginFile
        :param utility_name: name of IOU
        :param load_serving_entity: LoadServingEntity
        """
        # TODO: bulk create meters and intervalframes
        if utility_name == "PG&E":
            with transaction.atomic():
                for sa_id, values in origin_file.item_17_dict.items():
                    rate_plan_name = values["rate_plan_name"]
                    meter, _ = Meter.objects.get_or_create(
                        sa_id=sa_id,
                        rate_plan_name=rate_plan_name,
                        load_serving_entity=load_serving_entity,
                        origin_file=origin_file,
                    )
                    for (export, dataframe) in [
                        (False, values["import"]),
                        (True, values["export"]),
                    ]:
                        meter.get_or_create_channel(export, dataframe)
        elif utility_name == "SCE":
            pass
        elif utility_name == "SDG&E":
            pass
        else:
            pass

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

    export = models.BooleanField(default=False)
    data_unit = models.ForeignKey(
        to=DataUnit, related_name="channels", on_delete=models.PROTECT
    )
    meter = models.ForeignKey(
        to=Meter, related_name="channels", on_delete=models.CASCADE
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
    def meter_intervalframes(self):
        """
        QuerySet of all MeterIntervalFrame objects in associated
        CustomerCluster objects.
        """
        return reduce(
            lambda x, y: x | y,
            [
                x.meter_intervalframes.all()
                for x in self.customer_clusters.all()
            ],
            MeterIntervalFrame.objects.none(),
        )

    @property
    def meter_count(self):
        """
        Number of associated MeterIntervalFrame objects.
        """
        return self.meter_intervalframes.count()

    @classmethod
    def generate(
        cls,
        name,
        meter_intervalframes,
        frame288_type,
        number_of_clusters,
        normalize,
        load_serving_entity=None,
    ):
        """
        Create a CustomerPopulation and related CustomerClusters.

        :param name: name of CustomerPopulation
        :param meter_intervalframes: MeterIntervalFrame QuerySet
        :param frame288_type: choice - "average_frame288", "minimum_frame288",
            "maximum_frame288", "total_frame288", "count_frame288"
        :param number_of_clusters: number of clusters to create
        :param normalize: True to normalize all ValidationFrame288s to create
            values ranging between -1 and 1
        :param load_serving_entity: LoadServingEntity
        :return CustomerPopulation:
        """
        # return exising CustomerPopulation with same meter_intervalframes
        existing_populations = cls.objects.filter(
            frame288_type=frame288_type,
            normalize=normalize,
            customer_clusters__meter_intervalframes__in=meter_intervalframes,
        ).distinct()
        if existing_populations:
            return existing_populations.first()

        clustering = KMeansLoadClustering(
            objects=meter_intervalframes,
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
            cluster.meter_intervalframes.add(
                *clustering.get_objects_by_cluster_id(i)
            )

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
    meter_intervalframes = models.ManyToManyField(
        to=MeterIntervalFrame, related_name="customer_clusters"
    )

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
        return self.meter_intervalframes.count()

    @property
    def frame288_html_plot(self):
        """
        Return Django-formatted HTML frame288 plt.
        """
        return plot_frame288(frame288=self.frame288, to_html=True)
