from django.db import models

from reference.reference_model.models import DERSimulation


class RateDataMixin(object):
    """
    Mixin for all DER rate models used to generate DER cost-calculations.
    """

    @property
    def cost_calculation_model(self):
        """
        DERCostCalculation model associated with rate.
        """
        raise NotImplementedError(
            "cost_calculation_model must be defined in {}".format(
                self.__class__
            )
        )

    def rate_data(self):
        """
        Data structure containing rates (i.e. ValidationFrame288,
        ValidationIntervalFrame, dictionary, etc.)
        """
        raise NotImplementedError(
            "rate_data must be defined in {}".format(self.__class__)
        )

    def calculate_cost(self, der_simulation: DERSimulation, stacked: bool):
        """
        Perform DERCostCalculation.
        """
        if not isinstance(der_simulation, DERSimulation):
            raise TypeError(
                "{} must be a DERSimulation.".format(der_simulation)
            )

        if stacked:
            effective_der_simulation = der_simulation.stacked_der_simulation
        else:
            effective_der_simulation = der_simulation

        return self.cost_calculation_model(
            agg_simulation=effective_der_simulation.agg_simulation,
            rate_data=self.rate_data,
        )


class CostCalculationMixin(models.Model):
    """
    Mixin for all DER cost-calculation models.
    """

    stacked = models.BooleanField(default=True)

    class Meta:
        abstract = True

    @property
    def net_impact(self):
        """
        Return post-DER total minus pre-DER total.
        """
        return self.post_DER_total - self.pre_DER_total

    @property
    def effective_der_simulation(self):
        """
        DERSimulation used in cost calculation.
        """
        if self.stacked:
            return self.der_simulation.stacked_der_simulation
        else:
            return self.der_simulation
