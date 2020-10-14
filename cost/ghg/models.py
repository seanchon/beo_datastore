from functools import reduce
import os
import pandas as pd
import re

from django.db import models, transaction

from beo_datastore.libs.cost.controller import AggregateGHGCalculation
from beo_datastore.libs.load.intervalframe_file import Frame288File
from beo_datastore.libs.models import Frame288FileMixin, ValidationModel
from beo_datastore.settings import MEDIA_ROOT

from reference.reference_model.models import (
    CostCalculationMixin,
    DERSimulation,
    RateDataMixin,
    RateUnit,
)


class GHGRateFrame288(Frame288File):
    """
    Model for handling GHGRate Frame288Files.
    """

    # directory for parquet file storage
    file_directory = os.path.join(MEDIA_ROOT, "ghg_rates")


class GHGRate(Frame288FileMixin, RateDataMixin, ValidationModel):
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
    def cost_calculation_model(self):
        """
        Required by RateDataMixin.
        """
        return AggregateGHGCalculation

    @property
    def rate_data(self):
        """
        Required by RateDataMixin.
        """
        return self.frame288

    @property
    def short_name(self):
        ghg_rate_name = re.sub(r"\W+", "", self.name)
        return "{}{}".format(ghg_rate_name, self.effective.year)

    @property
    def dataframe(self):
        return self.frame288.dataframe


class StoredGHGCalculation(CostCalculationMixin, ValidationModel):
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
        unique_together = ("der_simulation", "ghg_rate", "stacked")

    @classmethod
    def generate(cls, der_simulation_set, ghg_rate, stacked):
        """
        Get or create many StoredGHGCalculations at once. Pre-existing
        StoredGHGCalculations are retrieved and non-existing
        StoredGHGCalculations are created.

        :param der_simulation_set: QuerySet or set of DERSimulations
        :param ghg_rate: GHGRate
        :param stacked: True to used StackedDERSimulation, False to use
            DERSimulation
        :return: StoredGHGCalculation QuerySet
        """
        with transaction.atomic():
            # get existing GHG calculations
            stored_ghg_calculations = cls.objects.filter(
                der_simulation__in=der_simulation_set,
                ghg_rate=ghg_rate,
                stacked=stacked,
            )

            # create new GHG calculations
            already_calculated = [
                x.der_simulation for x in stored_ghg_calculations
            ]
            objects = []
            for der_simulation in der_simulation_set:
                if der_simulation in already_calculated:
                    continue
                ghg_calculation = ghg_rate.calculate_cost(
                    der_simulation=der_simulation, stacked=stacked
                )
                objects.append(
                    cls(
                        pre_DER_total=ghg_calculation.pre_DER_total,
                        post_DER_total=ghg_calculation.post_DER_total,
                        der_simulation=der_simulation,
                        ghg_rate=ghg_rate,
                        stacked=stacked,
                    )
                )
            cls.objects.bulk_create(objects)

            return cls.objects.filter(
                der_simulation__in=der_simulation_set,
                ghg_rate=ghg_rate,
                stacked=stacked,
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
                            1: "GHGPreDER",
                            2: "GHGPostDER",
                            3: "GHGDelta",
                        }
                    ).set_index("ID")
                )

        return reduce(
            lambda x, y: x.join(y, how="outer", lsuffix="_0", rsuffix="_1"),
            dataframes,
            pd.DataFrame(),
        )
