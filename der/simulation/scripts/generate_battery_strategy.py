from datetime import date

from der.simulation.models import BatteryStrategy


GRID_CHARGE = "Battery is allowed to charge from the grid."
NEM_CHARGE = "Battery is only allowed to charge from NEM exports."
GRID_DISCHARGE = "Battery can discharge to the grid."
LOAD_DISCHARGE = "Battery can only discharge to offset meter's load."


def create_detailed_battery_description(battery_strategy):
    """
    Create a detailed text description of charge/discharge BatterySchedule.

    :param battery_strategy: BatteryStrategy
    :return: text
    """
    charge_schedule = battery_strategy.charge_schedule.frame288.dataframe
    discharge_schedule = battery_strategy.discharge_schedule.frame288.dataframe

    details = ""
    for i in range(1, 13):
        month = date(1900, i, 1).strftime("%B")
        details += month + "\n"

        nem_charge = [
            hour
            for hour, thresh in enumerate(charge_schedule[i])
            if thresh == 0
        ]
        if nem_charge:
            details += (
                "Charge (NEM Only): "
                + ", ".join(["{}:00".format(x) for x in nem_charge])
                + "\n"
            )

        grid_charge = [
            hour
            for hour, thresh in enumerate(charge_schedule[i])
            if thresh == float("inf")
        ]
        if grid_charge:
            details += (
                "Charge (Grid OK): "
                + ", ".join(["{}:00".format(x) for x in grid_charge])
                + "\n"
            )

        meter_discharge = [
            hour
            for hour, thresh in enumerate(discharge_schedule[i])
            if thresh == 0
        ]
        if meter_discharge:
            details += (
                "Discharge (No Export): "
                + ", ".join(["{}:00".format(x) for x in meter_discharge])
                + "\n"
            )

        grid_discharge = [
            hour
            for hour, thresh in enumerate(discharge_schedule[i])
            if thresh == float("-inf")
        ]
        if grid_discharge:
            details += (
                "Discharge (Export OK): "
                + ", ".join(["{}:00".format(x) for x in grid_discharge])
                + "\n"
            )

        details += "\n"

    return details


def generate_bill_reduction_battery_strategy(
    name, charge_grid, discharge_grid, rate_plan
):
    """
    Generate a BatteryStrategy with the intention of reducing a customer bill
    based on TOU-based rates in a 288 format. Overwrites name and description
    if BatteryStrategy exists.

    :param name: name of strategy (ex. TOU Default V1)
    :param charge_grid: True to allow charging from the grid, False to allow
        charging from NEM-exports only.
    :param discharge_grid: True to allow discharging to the grid, False to
        allow discharge of customer load only.
    :param rate_plan: RatePlan
    :return: BatteryStrategy
    """
    name = name + " (charge from grid: {}, discharge to grid: {})".format(
        str(bool(charge_grid)), str(bool(discharge_grid))
    )

    charge_description = GRID_CHARGE if charge_grid else NEM_CHARGE
    discharge_description = (
        GRID_DISCHARGE if discharge_grid else LOAD_DISCHARGE
    )

    charge_threshold = None if charge_grid else 0
    discharge_threshold = None if discharge_grid else 0

    latest_year = (
        rate_plan.rate_collections.order_by("effective_date")
        .last()
        .effective_date.year
    )

    # grid: charge during least expensive TOU period
    # NEM: charge from all but worst TOU period
    charge_aggresiveness = 1 if charge_grid else -1
    battery_strategy = BatteryStrategy.generate(
        name=name,
        description="\n".join([charge_description, discharge_description]),
        frame288=rate_plan.get_rate_frame288_by_year(
            latest_year, "energy", "weekday"
        ),
        charge_aggresiveness=charge_aggresiveness,
        discharge_aggresiveness=1,  # discharge during most expensive TOU period
        charge_threshold=charge_threshold,
        discharge_threshold=discharge_threshold,
        objective="reduce_bill",
    )

    # update description
    battery_strategy.description = (
        battery_strategy.description
        + "\n\n"
        + create_detailed_battery_description(battery_strategy)
    )
    battery_strategy.save()

    return battery_strategy


def generate_ghg_reduction_battery_strategy(
    name, charge_grid, discharge_grid, ghg_rate
):
    """
    Generate a BatteryStrategy with the intention of reducing GHG based on
    Clean Net Short tables in a 288 format. Overwrites name and description
    if BatteryStrategy exists.

    :param name: name of strategy (ex. CNS 2018)
    :param charge_grid: True to allow charging from the grid, False to allow
        charging from NEM-exports only.
    :param discharge_grid: True to allow discharging to the grid, False to
        allow discharge of customer load only.
    :param ghg_rate: GHGRate
    :return: BatteryStrategy
    """
    name = name + " (charge from grid: {}, discharge to grid: {})".format(
        str(bool(charge_grid)), str(bool(discharge_grid))
    )

    charge_description = GRID_CHARGE if charge_grid else NEM_CHARGE
    discharge_description = (
        GRID_DISCHARGE if discharge_grid else LOAD_DISCHARGE
    )

    charge_threshold = None if charge_grid else 0
    discharge_threshold = None if discharge_grid else 0

    battery_strategy = BatteryStrategy.generate(
        name=name,
        description="\n".join([charge_description, discharge_description]),
        frame288=ghg_rate.frame288,
        charge_aggresiveness=12,  # charge during 12 lowest GHG hours
        discharge_aggresiveness=8,  # discharge during 8 highest GHG hours
        charge_threshold=charge_threshold,
        discharge_threshold=discharge_threshold,
        objective="reduce_ghg",
    )

    # update description
    battery_strategy.description = (
        battery_strategy.description
        + "\n\n"
        + create_detailed_battery_description(battery_strategy)
    )
    battery_strategy.save()

    return battery_strategy


def generate_ra_reduction_battery_strategy(
    name, charge_grid, discharge_grid, system_profile
):
    """
    Generate a BatteryStrategy with the intention of reducting a CCA's system
    peak using a system profile maximum in a 288 format. Overwrites name and
    description if BatteryStrategy exists.

    :param name: name of strategy (ex. MCE 2018)
    :param charge_grid: True to allow charging from the grid, False to allow
        charging from NEM-exports only.
    :param discharge_grid: True to allow discharging to the grid, False to
        allow discharge of customer load only.
    :param system_profile: SystemProfile
    :return: BatteryStrategy
    """
    name = name + " (charge from grid: {}, discharge to grid: {})".format(
        str(bool(charge_grid)), str(bool(discharge_grid))
    )

    charge_description = GRID_CHARGE if charge_grid else NEM_CHARGE
    discharge_description = (
        GRID_DISCHARGE if discharge_grid else LOAD_DISCHARGE
    )

    charge_threshold = None if charge_grid else 0
    discharge_threshold = None if discharge_grid else 0

    # grid: charge during lowest 18 system profile hours
    # NEM: charge from all but worst 1 system profile hour
    charge_aggresiveness = 18 if charge_grid else -1
    battery_strategy = BatteryStrategy.generate(
        name=name,
        description="\n".join([charge_description, discharge_description]),
        frame288=system_profile.intervalframe.maximum_frame288,
        charge_aggresiveness=charge_aggresiveness,
        discharge_aggresiveness=1,  # discharge during 1 highest RA hour
        charge_threshold=charge_threshold,
        discharge_threshold=discharge_threshold,
        objective="reduce_cca_finance",
    )

    # update description
    battery_strategy.description = (
        battery_strategy.description
        + "\n\n"
        + create_detailed_battery_description(battery_strategy)
    )
    battery_strategy.save()

    return battery_strategy
