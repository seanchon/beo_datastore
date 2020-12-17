from datetime import date

from cost.ghg.models import GHGRate
from cost.procurement.models import SystemProfile
from cost.utility_rate.models import RatePlan
from der.simulation.models import BatteryStrategy, EVSEStrategy
from reference.auth_user.models import LoadServingEntity


GRID_CHARGE = "Battery is allowed to charge from the grid."
NEM_CHARGE = "Battery is only allowed to charge from NEM exports."
GRID_DISCHARGE = "Battery can discharge to the grid."
LOAD_DISCHARGE = "Battery can only discharge to offset meter's load."


def create_detailed_battery_description(battery_strategy: BatteryStrategy):
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


def set_battery_strategy_description(
    strategy: BatteryStrategy,
    user_description: str,
    charge_grid: bool,
    discharge_grid: bool,
):
    """
    Sets a battery strategy's description, provided with a user-provided
    description and some basic info about the battery's charging source and
    discharging strategy.

    :param strategy: the BatteryStrategy object
    :param user_description: a user-provided description
    :param charge_grid: True if the battery can charge from the grid
    :param discharge_grid: True if the battery can discharge to the grid
    """
    charge_description = GRID_CHARGE if charge_grid else NEM_CHARGE
    discharge_description = GRID_DISCHARGE if discharge_grid else LOAD_DISCHARGE
    default_description = (
        "\n".join([charge_description, discharge_description])
        + "\n\n"
        + create_detailed_battery_description(strategy)
    )

    strategy.description = user_description or default_description
    strategy.save()


def generate_bill_reduction_battery_strategy(
    name: str,
    charge_grid: bool,
    discharge_grid: bool,
    rate_plan: RatePlan,
    description: str = None,
    load_serving_entity: LoadServingEntity = None,
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
    :param description: description of the strategy
    :param load_serving_entity: the LSE to assign the BatteryStrategy to
    :return: BatteryStrategy
    """
    charge_threshold = None if charge_grid else 0
    discharge_threshold = None if discharge_grid else 0

    latest_year = (
        rate_plan.rate_collections.order_by("effective_date")
        .last()
        .effective_date.year
    )

    # grid: charge during least expensive TOU period
    # NEM: charge from all but worst TOU period
    charge_aggressiveness = 1 if charge_grid else -1
    battery_strategy = BatteryStrategy.generate(
        name=name,
        frame288=rate_plan.openei_rate_plan.get_rate_frame288_by_year(
            latest_year, "energy", "weekday"
        ),
        charge_aggressiveness=charge_aggressiveness,
        discharge_aggressiveness=1,  # discharge during most expensive TOU period
        charge_threshold=charge_threshold,
        discharge_threshold=discharge_threshold,
        objective="reduce_bill",
        load_serving_entity=load_serving_entity,
    )

    # update description
    set_battery_strategy_description(
        battery_strategy, description, charge_grid, discharge_grid
    )

    return battery_strategy


def generate_ghg_reduction_battery_strategy(
    name: str,
    charge_grid: bool,
    discharge_grid: bool,
    ghg_rate: GHGRate,
    description: str = None,
    load_serving_entity: LoadServingEntity = None,
):
    """
    Generate a BatteryStrategy with the intention of reducing GHG based on
    a GHGRate's 288 data. Overwrites name and description if BatteryStrategy
    exists.

    :param name: name of strategy (ex. CSP 2018)
    :param charge_grid: True to allow charging from the grid, False to allow
        charging from NEM-exports only.
    :param discharge_grid: True to allow discharging to the grid, False to
        allow discharge of customer load only.
    :param ghg_rate: GHGRate
    :param description: description of the strategy
    :param load_serving_entity: the LSE to assign the BatteryStrategy to
    :return: BatteryStrategy
    """
    charge_threshold = None if charge_grid else 0
    discharge_threshold = None if discharge_grid else 0

    battery_strategy = BatteryStrategy.generate(
        name=name,
        frame288=ghg_rate.frame288,
        charge_aggressiveness=12,  # charge during 12 lowest GHG hours
        discharge_aggressiveness=8,  # discharge during 8 highest GHG hours
        charge_threshold=charge_threshold,
        discharge_threshold=discharge_threshold,
        objective="reduce_ghg",
        load_serving_entity=load_serving_entity,
    )

    # update description
    set_battery_strategy_description(
        battery_strategy, description, charge_grid, discharge_grid
    )

    return battery_strategy


def generate_ra_reduction_battery_strategy(
    name: str,
    charge_grid: bool,
    discharge_grid: bool,
    system_profile: SystemProfile,
    description: str = None,
    load_serving_entity: LoadServingEntity = None,
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
    :param description: description of the strategy
    :param load_serving_entity: the LSE to assign the BatteryStrategy to
    :return: BatteryStrategy
    """
    charge_threshold = None if charge_grid else 0
    discharge_threshold = None if discharge_grid else 0

    # grid: charge during lowest 18 system profile hours
    # NEM: charge from all but worst 1 system profile hour
    charge_aggressiveness = 18 if charge_grid else -1
    battery_strategy = BatteryStrategy.generate(
        name=name,
        frame288=system_profile.intervalframe.maximum_frame288,
        charge_aggressiveness=charge_aggressiveness,
        discharge_aggressiveness=1,  # discharge during 1 highest RA hour
        charge_threshold=charge_threshold,
        discharge_threshold=discharge_threshold,
        objective="reduce_cca_finance",
        load_serving_entity=load_serving_entity,
    )

    # update description
    set_battery_strategy_description(
        battery_strategy, description, charge_grid, discharge_grid
    )

    return battery_strategy


def generate_commuter_evse_strategy(
    charge_off_nem: bool,
    start_charge_hour: int,
    end_charge_hour: int,
    distance: float,
    name: str,
    user_description: str = None,
    load_serving_entity: LoadServingEntity = None,
):
    """
    Generates a description for a new EVSEStrategy if one is not provided.
    """
    description = user_description or (
        "Vehicles begin charging at {start_charge_hour} and end charging at "
        "{end_charge_hour}. In between charges the vehicles are assumed to "
        "commute {distance} miles one way.".format(
            distance=distance,
            start_charge_hour=format_hour(start_charge_hour),
            end_charge_hour=format_hour(end_charge_hour),
        )
    )

    return EVSEStrategy.generate(
        charge_off_nem=charge_off_nem,
        description=description,
        distance=distance,
        end_charge_hour=end_charge_hour,
        start_charge_hour=start_charge_hour,
        load_serving_entity=load_serving_entity,
        name=name,
    )


def format_hour(hour: int) -> str:
    """
    Formats an hour provided as an integer between 0 and 23 into a string
    representation.

      Ex:
        0  --> 12 am
        5  -->  5 am
        12 --> 12 pm
        18 -->  6 pm
        23 --> 11 pm

    :param hour: int
    """
    if hour < 12:
        return "{hour} am".format(hour=hour if hour != 0 else 12)
    else:
        return "{hour} pm".format(hour=hour if hour == 12 else hour - 12)
