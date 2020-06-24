from functools import reduce
import os
import pandas as pd
import re

from django.db import models, transaction
from django.utils.functional import cached_property

from beo_datastore.libs.controller import AggregateGHGCalculation
from beo_datastore.libs.intervalframe_file import Frame288File
from beo_datastore.libs.models import ValidationModel, Frame288FileMixin
from beo_datastore.settings import MEDIA_ROOT
from beo_datastore.libs.views import dataframe_to_html

from reference.reference_model.models import DERSimulation, RateUnit


class GHGRateFrame288(Frame288File):
    """
    Model for handling GHGRate Frame288Files.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "ghg_rates")


class GHGRate(Frame288FileMixin, ValidationModel):
    """
    Provides lookup-values for GHG emissions calculations.
    """

    name = models.CharField(max_length=32)
    effective = models.DateField(blank=True, null=True)
    source = models.URLField(max_length=128, blank=True, null=True)
    rate_unit = models.ForeignKey(
        to=RateUnit, related_name="ghg_rates", on_delete=models.PROTECT
    )

    # Required by Frame288FileMixin.
    frame_file_class = GHGRateFrame288

    class Meta:
        ordering = ["id"]
        unique_together = ("name", "effective")

    def __str__(self):
        if self.effective:
            return "{} effective: {} ({})".format(
                self.name, self.effective, self.rate_unit
            )
        else:
            return "{} ({})".format(self.name, self.rate_unit)

    @property
    def short_name(self):
        ghg_rate_name = re.sub(r"\W+", "", self.name)
        return "{}{}".format(ghg_rate_name, self.effective.year)

    @property
    def dataframe(self):
        return self.frame288.dataframe


class StoredGHGCalculation(ValidationModel):
    """
    Container for storing AggregateGHGCalculation.
    """

    pre_DER_total = models.FloatField()
    post_DER_total = models.FloatField()
    der_simulation = models.ForeignKey(
        to=DERSimulation,
        related_name="stored_ghg_calculations",
        on_delete=models.CASCADE,
    )
    ghg_rate = models.ForeignKey(
        to=GHGRate,
        related_name="stored_ghg_calculations",
        on_delete=models.CASCADE,
    )

    class Meta:
        ordering = ["id"]
        unique_together = ("der_simulation", "ghg_rate")

    @property
    def net_impact(self):
        """
        Return post-DER total minus pre-DER total.
        """
        return self.post_DER_total - self.pre_DER_total

    @property
    def comparision_html_table(self):
        """
        Return Django-formatted HTML pre vs. post comparision table.
        """
        return dataframe_to_html(
            self.aggregate_ghg_calculation.comparison_table
        )

    @cached_property
    def aggregate_ghg_calculation(self):
        """
        Return AggregateGHGCalculation equivalent of self.
        """
        return AggregateGHGCalculation(
            agg_simulation=self.der_simulation.agg_simulation,
            ghg_frame288=self.ghg_rate.frame288,
        )

    @classmethod
    def generate(cls, der_simulation_set, ghg_rate):
        """
        Get or create many StoredGHGCalculations at once. Pre-existing
        StoredGHGCalculations are retrieved and non-existing
        StoredGHGCalculations are created.

        :param der_simulation_set: QuerySet or set of DERSimulations
        :param ghg_rate: GHGRate
        :return: StoredGHGCalculation QuerySet
        """
        with transaction.atomic():
            # get existing GHG calculations
            stored_ghg_calculations = cls.objects.filter(
                der_simulation__in=der_simulation_set, ghg_rate=ghg_rate
            )

            # create new GHG calculations
            stored_simulations = [
                x.der_simulation for x in stored_ghg_calculations
            ]
            objects = []
            for der_simulation in der_simulation_set:
                if der_simulation in stored_simulations:
                    continue
                ghg_calculation = AggregateGHGCalculation(
                    agg_simulation=der_simulation.agg_simulation,
                    ghg_frame288=ghg_rate.frame288,
                )
                objects.append(
                    cls(
                        pre_DER_total=ghg_calculation.pre_DER_total,
                        post_DER_total=ghg_calculation.post_DER_total,
                        der_simulation=der_simulation,
                        ghg_rate=ghg_rate,
                    )
                )
            cls.objects.bulk_create(objects)

            return cls.objects.filter(
                der_simulation__in=der_simulation_set, ghg_rate=ghg_rate
            )

    @staticmethod
    def get_report(ghg_calculations):
        """
        Return pandas DataFrame in the format:

        |   ID  |   GHGPreDER   |   GHGPostDER  |   GHGDelta    |

        :param ghg_calculations: QuerySet or set of StoredGHGCalculations
        :return: pandas DataFrame
        """
        ghg_rate_ids = (
            ghg_calculations.values_list("ghg_rate", flat=True)
            .order_by()
            .distinct()
        )

        dataframes = []
        for ghg_rate_id in ghg_rate_ids:
            ghg_rate = GHGRate.objects.get(id=ghg_rate_id)

            dataframe = pd.DataFrame(
                sorted(
                    [
                        (
                            x.der_simulation.meter.id,
                            x.pre_DER_total,
                            x.post_DER_total,
                            x.net_impact,
                        )
                        for x in ghg_calculations.filter(ghg_rate=ghg_rate)
                    ],
                    key=lambda x: x[1],
                )
            )

            if not dataframe.empty:
                dataframes.append(
                    dataframe.rename(
                        columns={
                            0: "ID",
                            1: "{}PreDER".format(ghg_rate.short_name),
                            2: "{}PostDER".format(ghg_rate.short_name),
                            3: "{}Delta".format(ghg_rate.short_name),
                        }
                    ).set_index("ID")
                )

        return reduce(
            lambda x, y: x.join(y, how="outer", lsuffix="_0", rsuffix="_1"),
            dataframes,
            pd.DataFrame(),
        )
