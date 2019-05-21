import pandas as pd

from beo_datastore.libs.intervalframe import ValidationIntervalFrame


class Battery(object):
    def __init__(
        self, rating, discharge_time, efficiency, charge=0, *args, **kwargs
    ):
        """
        Initialize a battery with a rating (kw), discharge time (hours),
        efficiency (percentage), and charge.

        A fully discharged battery can fully charge:
            - at rating (kw) for (discharge time / efficiency) (hours)

        A fully charged battery can fully discharge:
            - at rating (kw) for discharge time (hours)

        :param rating: float (kw)
        :param discharge_time: float (hour)
        :param charge: float (kwh)
        :param efficiency: float (0 to 1)
        """
        self.rating = rating
        self.discharge_time = discharge_time
        self.efficiency = efficiency
        self.charge = charge

    @property
    def charge(self):
        return self._charge

    @charge.setter
    def charge(self, charge):
        self.validate_charge(charge)
        self._charge = charge

    @property
    def max_capacity(self):
        """
        Maximum capacity a battery has available for discharge.
        """
        return self.rating * self.discharge_time

    @property
    def state_of_charge(self):
        """
        Charge available divided by the maximum capacity.
        """
        return self.charge / self.max_capacity

    def validate_charge(self, charge):
        """
        Ensures that battery charge state is neither less than zero or greater
        that its max capacity.
        """
        if charge < 0:
            raise AttributeError("Charge cannot drop below 0.")
        elif charge > self.max_capacity:
            raise AttributeError(
                "Charge cannot exceed max capacity - {} kwh.".format(
                    self.max_capacity
                )
            )

    def validate_power(self, power):
        """
        Ensures that power level is neither less than zero or greater than the
        battery's rating.
        """
        if not (0 <= power <= self.rating):
            raise AttributeError(
                "Power must be between 0 and {} kw".format(self.rating)
            )

    def charge_battery(self, power, time):
        """
        Charge a battery at power level (kw) for an amount of time (hour).

        :param power: kw
        :param time: hour
        """
        self.validate_power(power)
        self.charge += power * time * self.efficiency

    def discharge_battery(self, power, time):
        """
        Discharge battery for a number of hours and return discharge amount.

        :param power: kw
        :param time: hour
        :return: discharge amount (kwh)
        """
        self.validate_power(power)
        self.charge -= power * time
        return power * time

    def get_full_charge_time(self, power):
        """
        Return the amount of time to get to full charge based on input power.

        :param power: kw
        :return: charge time (hour)
        """
        return (self.max_capacity - self.charge) / (power * self.efficiency)

    def get_full_charge_power(self, time):
        """
        Return the power level to get to full charge based on input time.

        :param time: hour
        :return: charge power (kw)
        """
        return (self.max_capacity - self.charge) / (time * self.efficiency)

    def get_full_discharge_time(self, power):
        """
        Return the amount of time to get to full discharge based on input
        power.

        :param power: kw
        :return: charge time (hour)
        """
        return self.charge / power

    def get_full_discharge_power(self, time):
        """
        Return the power level to get to full discharge based on input time.

        :param time: hour
        :return: charge power (kw)
        """
        return self.charge / time


class BatteryIntervalFrame(ValidationIntervalFrame):
    default_dataframe = pd.DataFrame(
        columns=["kw", "state of charge"], index=pd.to_datetime([])
    )

    def __init__(self, battery, load_intervalframe, *args, **kwargs):
        self.battery = battery
        self.load_intervalframe = load_intervalframe
        super().__init__(self.default_dataframe, *args, **kwargs)
