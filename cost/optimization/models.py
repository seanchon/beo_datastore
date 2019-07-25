from functools import reduce
import pandas as pd

from django.db import models
from django.utils.functional import cached_property

from beo_datastore.libs.models import ValidationModel

from cost.ghg.models import GHGRate, StoredGHGCalculation
from cost.utility_rate.models import RatePlan, StoredBillCalculation
from der.simulation.models import (
    BatterySchedule,
    BatteryConfiguration,
    StoredBatterySimulation,
)
from load.customer.models import Meter


class BatterySimulationOptimization(ValidationModel):
    start = models.DateTimeField()
    end_limit = models.DateTimeField()
    charge_schedule = models.ForeignKey(
        to=BatterySchedule,
        related_name="charge_schedule_battery_simulation_optimizations",
        on_delete=models.CASCADE,
    )
    discharge_schedule = models.ForeignKey(
        to=BatterySchedule,
        related_name="discharge_schedule_battery_simulation_optimizations",
        on_delete=models.CASCADE,
    )
    battery_configuration = models.ForeignKey(
        to=BatteryConfiguration,
        related_name="battery_simulation_optimizations",
        on_delete=models.CASCADE,
    )
    rate_plan = models.ForeignKey(
        to=RatePlan,
        related_name="battery_simulation_optimizations",
        on_delete=models.CASCADE,
    )
    ghg_rates = models.ManyToManyField(to=GHGRate)
    meters = models.ManyToManyField(to=Meter)

    @property
    def battery_simulations(self):
        """
        Return StoredBatterySimulations related to self.
        """
        return StoredBatterySimulation.generate(
            battery=self.battery_configuration.battery,
            start=self.start,
            end_limit=self.end_limit,
            meter_set=self.meters.all(),
            charge_schedule=self.charge_schedule.frame288,
            discharge_schedule=self.discharge_schedule.frame288,
        )

    @property
    def bill_calculations(self):
        """
        Return StoredBillCalculations related to self.
        """
        return StoredBillCalculation.generate(
            battery_simulation_set=self.battery_simulations,
            rate_plan=self.rate_plan,
        )

    @property
    def ghg_calculations(self):
        """"
        Return StoredGHGCalculations related to self.
        """
        ghg_calculations = []
        for ghg_rate in self.ghg_rates.all():
            ghg_calculations.append(
                StoredGHGCalculation.generate(
                    battery_simulation_set=self.battery_simulations,
                    ghg_rate=ghg_rate,
                )
            )

        return reduce(
            lambda x, y: x | y,
            ghg_calculations,
            StoredGHGCalculation.objects.none(),
        )

    @cached_property
    def report(self):
        """
        Return pandas Dataframe with meter SA IDs and all bill and GHG impacts.
        """
        return self.bill_report.join(self.ghg_report, how="outer")

    @cached_property
    def bill_report(self):
        """
        Return pandas DataFrame with meter SA IDs and bill impacts.
        """
        return (
            pd.DataFrame(
                sorted(
                    [
                        (x.battery_simulation.meter.sa_id, x.net_impact)
                        for x in self.bill_calculations
                    ],
                    key=lambda x: x[1],
                )
            )
            .rename(columns={0: "SA ID", 1: "Bill Impact"})
            .set_index("SA ID")
        )

    @cached_property
    def ghg_report(self):
        """
        Return pandas DataFrame with meter SA IDs and GHG impacts from all
        associated GHGRates.
        """
        return reduce(
            lambda x, y: x.join(y, how="outer"),
            [
                self.get_ghg_report(ghg_rate)
                for ghg_rate in self.ghg_rates.all()
            ],
            pd.DataFrame(),
        )

    @cached_property
    def agg_simulation(self):
        """
        Return AggregateBatterySimulation equivalent of self.
        """
        return reduce(
            lambda x, y: x + y,
            [x.agg_simulation for x in self.battery_simulations],
        )

    def get_ghg_report(self, ghg_rate):
        """
        Return pandas DataFrame with meter SA IDs and GHG impacts.

        :param ghg_rate: GHGRate
        :return: pandas DataFrame
        """
        return (
            pd.DataFrame(
                sorted(
                    [
                        (x.battery_simulation.meter.sa_id, x.net_impact)
                        for x in self.ghg_calculations.filter(
                            ghg_rate=ghg_rate
                        )
                    ],
                    key=lambda x: x[1],
                )
            )
            .rename(
                columns={
                    0: "SA ID",
                    1: "{} {} Impact".format(
                        ghg_rate.effective.year, ghg_rate.name
                    ),
                }
            )
            .set_index("SA ID")
        )

    def run(self, multiprocess=False):
        """
        Run related StoredBatterySimulations, StoredBillCalculations and
        StoredGHGCalculations.

        Note: Meters and GHGRates need to be added to object prior to
        optimization.

        :param multiprocess: True to multiprocess
        """
        battery_simulation_set = StoredBatterySimulation.generate(
            battery=self.battery_configuration.battery,
            start=self.start,
            end_limit=self.end_limit,
            meter_set=self.meters.all(),
            charge_schedule=self.charge_schedule.frame288,
            discharge_schedule=self.discharge_schedule.frame288,
            multiprocess=multiprocess,
        )

        StoredBillCalculation.generate(
            battery_simulation_set=battery_simulation_set,
            rate_plan=self.rate_plan,
            multiprocess=multiprocess,
        )

        for ghg_rate in self.ghg_rates.all():
            StoredGHGCalculation.generate(
                battery_simulation_set=battery_simulation_set,
                ghg_rate=ghg_rate,
            )
