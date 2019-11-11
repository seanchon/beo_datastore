from functools import reduce
import os
import us

from django.db import connection, models
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

from reference.reference_model.models import DataUnit, LoadServingEntity


class Meter(ValidationModel):
    """
    A Meter is a connection point to the Utility's distribution grid
    identified by a Service Address Identifier (sa_id).
    """

    sa_id = models.IntegerField(db_index=True, unique=True)
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
    def intervalframe_html_plot(self):
        """
        Return Django-formatted HTML intervalframe plt.
        """
        return plot_intervalframe(
            intervalframe=self.intervalframe, y_label="kw", to_html=True
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
        Return a 12 x 24 dataframe of peaks.
        """
        return self.intervalframe.minimum_frame288.dataframe

    @property
    def count_288(self):
        """
        Return a 12 x 24 dataframe of counts.
        """
        return self.intervalframe.count_frame288.dataframe

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


class ChannelQuerySet(models.QuerySet):
    """
    Overloads QuerySet operations for bulk file-handling.
    """

    def delete(self, *args, **kwargs):
        """
        Bulk delete IntervalFrameFile files from disk.
        """
        # TODO: Create a quicker cleanup method.
        for obj in self:
            obj.intervalframe.delete()
        super().delete(*args, **kwargs)


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

    # custom QuerySet manager for frame file-handling
    objects = ChannelQuerySet.as_manager()

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
    )

    class Meta:
        ordering = ["id"]
        unique_together = [
            "name",
            "frame288_type",
            "normalize",
            "load_serving_entity",
        ]

    def __str__(self):
        normalized = " normalize" if self.normalize else ""
        return "{}: {} {}{} ({} clusters)".format(
            self.load_serving_entity.name,
            self.name,
            self.frame288_type,
            normalized,
            self.number_of_clusters,
        )

    @property
    def number_of_clusters(self):
        return self.customer_clusters.count()

    @property
    def number_of_meters(self):
        return sum([x.meters.count() for x in self.customer_clusters.all()])

    @classmethod
    def generate(
        cls,
        load_serving_entity,
        meters,
        frame288_type,
        number_of_clusters,
        normalize,
    ):
        """
        Create a CustomerPopulation and related CustomerClusters.

        :param load_serving_entity: LoadServingEntity
        :param meters: Meter QuerySet
        :param frame288_type: choice - "average_frame288", "minimum_frame288",
            "maximum_frame288", "total_frame288", "count_frame288"
        :param number_of_clusters: number of clusters to create
        :param normalize: True to normalize all ValidationFrame288s to create
            values ranging between -1 and 1
        :return CustomerPopulation:
        """
        # filter Meters to those of a particular LoadServingEntity
        meters = meters.filter(load_serving_entity=load_serving_entity)

        # generate name based on Meters' RatePlans and count
        name = " ".join(
            meters.order_by()
            .values_list("rate_plan_name", flat=True)
            .distinct()
        )
        name += " (count: {})".format(meters.count())

        # return existing population if one already exists
        existing_populations = cls.objects.filter(
            name=name,
            frame288_type=frame288_type,
            normalize=normalize,
            load_serving_entity=load_serving_entity,
        )
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
        return "{}: {} {}{} ({} of {}, ID: {})".format(
            self.customer_population.load_serving_entity.name,
            self.customer_population.name,
            self.customer_population.frame288_type,
            normalized,
            self.cluster_id,
            self.customer_population.number_of_clusters,
            self.id,
        )

    @property
    def number_of_meters(self):
        return self.meters.count()

    @property
    def frame288_html_plot(self):
        """
        Return Django-formatted HTML frame288 plt.
        """
        return plot_frame288(frame288=self.frame288, to_html=True)
