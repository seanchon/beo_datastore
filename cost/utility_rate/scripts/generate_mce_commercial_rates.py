from datetime import datetime
import json


SOURCE = (
    "https://www.mcecleanenergy.org/wp-content/uploads/2019/01/"
    "MCE-Commercial-Rates-updated-1.23.19-FINAL.pdf"
)

BASIC_SCHED = [[0] * 24] * 12

# A1 and A10
# 0 - Summer, 1 - Winter

A1_A10_SCHED = [
    [1] * 24,
    [1] * 24,
    [1] * 24,
    [1] * 24,
    [0] * 24,
    [0] * 24,
    [0] * 24,
    [0] * 24,
    [0] * 24,
    [0] * 24,
    [1] * 24,
    [1] * 24,
]
A1_10_FLAT_DEMAND_SCHED = [1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1]

# Default V1: (note: time periods at the :30 mark have been moved to the
# preceding :00 mark)
# 0 - Summer Peak, 1 - Summer Part-Peak, 2 - Summer Off-Peak,
# 3 - Winter Part-Peak, 4 - Winter Off-Peak

DEFAULT_V1_WEEKDAY_SCHEDULE = [
    [4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4],
    [2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2],
    [4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4],
]
DEFAULT_V1_WEEKEND_SCHEDULE = [
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4],
]


def create_energy_dict(
    energy_key_vals=None,
    energy_max=None,
    energy_rate_strux=None,
    energy_weekday_schedule=None,
    energy_weekend_schedule=None,
):
    energy_dict = {
        "energyKeyVals": energy_key_vals,
        "energyMax": energy_max,
        "energyRateStrux": energy_rate_strux,
        "energyWeekdaySched": energy_weekday_schedule,
        "energyWeekendSched": energy_weekend_schedule,
    }
    return {x: y for x, y in energy_dict.items() if y is not None}


def create_demand_dict(
    demand_key_vals=None,
    demand_max=None,
    demand_min=None,
    demand_rate_strux=None,
    demand_rate_units=None,
    demand_units=None,
    demand_weekday_schedule=None,
    demand_weekend_schedule=None,
    flat_demand_months=None,
    flat_demand_strux=None,
    flat_demand_units=None,
):
    demand_dict = {
        "demandKeyVals": demand_key_vals,
        "demandMax": demand_max,
        "demandMin": demand_min,
        "demandRateStrux": demand_rate_strux,
        "demandRateUnits": demand_rate_units,
        "demandUnits": demand_units,
        "demandWeekdaySched": demand_weekday_schedule,
        "demandWeekendSched": demand_weekend_schedule,
        "flatDemandMonths": flat_demand_months,
        "flatDemandStrux": flat_demand_strux,
        "flatDemandUnits": flat_demand_units,
    }
    return {x: y for x, y in demand_dict.items() if y is not None}


def run(*args):
    """
    Usage:
        - python manage.py runscript cost.utility_rate.scripts.generate_mce_commercial_rates --script-args DESTINATION
    """
    if len(args) < 1:
        print(
            "USAGE `python manage.py runscript "
            "cost.utility_rate.scripts.generate_mce_residential_rates "
            "--script-args DESTINATION`"
        )
        return
    else:
        destination = args[0]

    rate_data = []

    # Create energy rates
    for (
        name,
        energy_rates,
        energy_weekday_sched,
        energy_weekend_sched,
        demand_rates,
        demand_weekday_schedule,
        demand_weekend_schedule,
        flat_demand_rates,
        flat_demand_sched,
    ) in [
        (
            "A1, Small General Service",
            [{"key": "Summer", "val": 0.092}, {"key": "Winter", "val": 0.057}],
            A1_A10_SCHED,
            A1_A10_SCHED,
            [],
            None,
            None,
            [],
            None,
        ),
        (
            "A1X, Small General Service with Time-of-Use (TOU)",
            [
                {"key": "Summer Peak", "val": 0.108},
                {"key": "Summer Part-Peak", "val": 0.086},
                {"key": "Summer Off-Peak", "val": 0.057},
                {"key": "Winter Part-Peak", "val": 0.084},
                {"key": "Winter Off-Peak", "val": 0.063},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [],
            None,
        ),
        (
            "A6, Small General Service with TOU",
            [
                {"key": "Summer Peak", "val": 0.340},
                {"key": "Summer Part-Peak", "val": 0.102},
                {"key": "Summer Off-Peak", "val": 0.045},
                {"key": "Winter Part-Peak", "val": 0.071},
                {"key": "Winter Off-Peak", "val": 0.052},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [],
            None,
        ),
        (
            "A10, Medium General Service",
            [{"key": "Summer", "val": 0.081}, {"key": "Winter", "val": 0.058}],
            A1_A10_SCHED,
            A1_A10_SCHED,
            [],
            None,
            None,
            [{"key": "Summer", "val": 4.85}, {"key": "Winter", "val": 0}],
            A1_10_FLAT_DEMAND_SCHED,
        ),
        (
            "A10X, Medium General Service with TOU",
            [
                {"key": "Summer Peak", "val": 0.135},
                {"key": "Summer Part-Peak", "val": 0.082},
                {"key": "Summer Off-Peak", "val": 0.054},
                {"key": "Winter Part-Peak", "val": 0.065},
                {"key": "Winter Off-Peak", "val": 0.049},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [{"key": "Summer", "val": 4.85}, {"key": "Winter", "val": 0}],
            A1_10_FLAT_DEMAND_SCHED,
        ),
        (
            "E19, Medium General Service, Secondary",
            [
                {"key": "Summer Peak", "val": 0.105},
                {"key": "Summer Part-Peak", "val": 0.065},
                {"key": "Summer Off-Peak", "val": 0.040},
                {"key": "Winter Part-Peak", "val": 0.059},
                {"key": "Winter Off-Peak", "val": 0.045},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [
                {"key": "Summer Peak Demand", "val": 12.60},
                {"key": "Summer Part-Peak Demand", "val": 3.10},
                {"key": "Summer Off-Peak Demand", "val": 0},
                {"key": "Winter Part-Peak Demand", "val": 0},
                {"key": "Winter Off-Peak Demand", "val": 0},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
        ),
        (
            "E19, Medium General Service, Primary",
            [
                {"key": "Summer Peak", "val": 0.097},
                {"key": "Summer Part-Peak", "val": 0.058},
                {"key": "Summer Off-Peak", "val": 0.035},
                {"key": "Winter Part-Peak", "val": 0.053},
                {"key": "Winter Off-Peak", "val": 0.040},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [
                {"key": "Summer Peak Demand", "val": 11.25},
                {"key": "Summer Part-Peak Demand", "val": 2.75},
                {"key": "Summer Off-Peak Demand", "val": 0},
                {"key": "Winter Part-Peak Demand", "val": 0},
                {"key": "Winter Off-Peak Demand", "val": 0},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
        ),
        (
            "E19, Medium General Service, Transmission",
            [
                {"key": "Summer Peak", "val": 0.060},
                {"key": "Summer Part-Peak", "val": 0.048},
                {"key": "Summer Off-Peak", "val": 0.034},
                {"key": "Winter Part-Peak", "val": 0.050},
                {"key": "Winter Off-Peak", "val": 0.038},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [
                {"key": "Summer Peak Demand", "val": 12.40},
                {"key": "Summer Part-Peak Demand", "val": 3.10},
                {"key": "Summer Off-Peak Demand", "val": 0},
                {"key": "Winter Part-Peak Demand", "val": 0},
                {"key": "Winter Off-Peak Demand", "val": 0},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
        ),
        (
            "E19R, Medium General Service, Secondary",
            [
                {"key": "Summer Peak", "val": 0.240},
                {"key": "Summer Part-Peak", "val": 0.095},
                {"key": "Summer Off-Peak", "val": 0.039},
                {"key": "Winter Part-Peak", "val": 0.060},
                {"key": "Winter Off-Peak", "val": 0.045},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [],
            None,
        ),
        (
            "E19R, Medium General Service, Primary",
            [
                {"key": "Summer Peak", "val": 0.230},
                {"key": "Summer Part-Peak", "val": 0.088},
                {"key": "Summer Off-Peak", "val": 0.034},
                {"key": "Winter Part-Peak", "val": 0.053},
                {"key": "Winter Off-Peak", "val": 0.040},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [],
            None,
        ),
        (
            "E19R, Medium General Service, Transmission",
            [
                {"key": "Summer Peak", "val": 0.230},
                {"key": "Summer Part-Peak", "val": 0.085},
                {"key": "Summer Off-Peak", "val": 0.032},
                {"key": "Winter Part-Peak", "val": 0.050},
                {"key": "Winter Off-Peak", "val": 0.038},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [],
            None,
        ),
        (
            "E20, Large General Service, Secondary",
            [
                {"key": "Summer Peak", "val": 0.095},
                {"key": "Summer Part-Peak", "val": 0.060},
                {"key": "Summer Off-Peak", "val": 0.036},
                {"key": "Winter Part-Peak", "val": 0.055},
                {"key": "Winter Off-Peak", "val": 0.041},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [
                {"key": "Summer Peak Demand", "val": 12.20},
                {"key": "Summer Part-Peak Demand", "val": 3.00},
                {"key": "Summer Off-Peak Demand", "val": 0},
                {"key": "Winter Part-Peak Demand", "val": 0},
                {"key": "Winter Off-Peak Demand", "val": 0},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
        ),
        (
            "E20, Large General Service, Primary",
            [
                {"key": "Summer Peak", "val": 0.102},
                {"key": "Summer Part-Peak", "val": 0.061},
                {"key": "Summer Off-Peak", "val": 0.037},
                {"key": "Winter Part-Peak", "val": 0.056},
                {"key": "Winter Off-Peak", "val": 0.042},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [
                {"key": "Summer Peak Demand", "val": 13.40},
                {"key": "Summer Part-Peak Demand", "val": 3.15},
                {"key": "Summer Off-Peak Demand", "val": 0},
                {"key": "Winter Part-Peak Demand", "val": 0},
                {"key": "Winter Off-Peak Demand", "val": 0},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
        ),
        (
            "E20, Large General Service, Transmission",
            [
                {"key": "Summer Peak", "val": 0.062},
                {"key": "Summer Part-Peak", "val": 0.049},
                {"key": "Summer Off-Peak", "val": 0.034},
                {"key": "Winter Part-Peak", "val": 0.051},
                {"key": "Winter Off-Peak", "val": 0.039},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [
                {"key": "Summer Peak Demand", "val": 15.85},
                {"key": "Summer Part-Peak Demand", "val": 3.75},
                {"key": "Summer Off-Peak Demand", "val": 0},
                {"key": "Winter Part-Peak Demand", "val": 0},
                {"key": "Winter Off-Peak Demand", "val": 0},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
        ),
        (
            "E20R, Large General Service, Secondary",
            [
                {"key": "Summer Peak", "val": 0.220},
                {"key": "Summer Part-Peak", "val": 0.090},
                {"key": "Summer Off-Peak", "val": 0.037},
                {"key": "Winter Part-Peak", "val": 0.055},
                {"key": "Winter Off-Peak", "val": 0.042},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [],
            None,
        ),
        (
            "E20R, Large General Service, Primary",
            [
                {"key": "Summer Peak", "val": 0.240},
                {"key": "Summer Part-Peak", "val": 0.089},
                {"key": "Summer Off-Peak", "val": 0.037},
                {"key": "Winter Part-Peak", "val": 0.055},
                {"key": "Winter Off-Peak", "val": 0.042},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [],
            None,
        ),
        (
            "E20R, Large General Service, Transmission",
            [
                {"key": "Summer Peak", "val": 0.230},
                {"key": "Summer Part-Peak", "val": 0.084},
                {"key": "Summer Off-Peak", "val": 0.035},
                {"key": "Winter Part-Peak", "val": 0.051},
                {"key": "Winter Off-Peak", "val": 0.040},
            ],
            DEFAULT_V1_WEEKDAY_SCHEDULE,
            DEFAULT_V1_WEEKEND_SCHEDULE,
            [],
            None,
            None,
            [],
            None,
        ),
    ]:
        energy_key_vals = []
        energy_rate_strux = []
        for i, rate in enumerate(energy_rates):
            energy_key_vals.append({"key": rate["key"], "val": i + 1})
            energy_rate_strux.append(
                {
                    "energyRateTiers": [
                        {"unit": "kWh", "rate": energy_rates[i]["val"]}
                    ]
                }
            )
        energy_dict = create_energy_dict(
            energy_key_vals=energy_key_vals,
            energy_rate_strux=energy_rate_strux,
            energy_weekday_schedule=energy_weekday_sched,
            energy_weekend_schedule=energy_weekend_sched,
        )

        flat_demand_rate_strux = []
        if flat_demand_rates:
            for i, rate in enumerate(flat_demand_rates):
                flat_demand_rate_strux.append(
                    {
                        "flatDemandTiers": [
                            {"rate": flat_demand_rates[i]["val"]}
                        ]
                    }
                )
            flat_demand_dict = create_demand_dict(
                flat_demand_months=flat_demand_sched,
                flat_demand_strux=flat_demand_rate_strux,
                flat_demand_units="kW",
            )
            energy_dict.update(flat_demand_dict)

        demand_rate_strux = []
        energy_dict["rateName"] = name
        if demand_rates:
            for i, rate in enumerate(demand_rates):
                demand_rate_strux.append(
                    {"demandRateTiers": [{"rate": demand_rates[i]["val"]}]}
                )
            demand_dict = create_demand_dict(
                demand_weekday_schedule=demand_weekday_schedule,
                demand_weekend_schedule=demand_weekend_schedule,
                demand_rate_strux=demand_rate_strux,
                demand_rate_units="kW",
            )
            energy_dict.update(demand_dict)

        rate_data.append(energy_dict)

    # add metadata
    for i, _ in enumerate(rate_data):
        rate_data[i]["approved"] = True
        rate_data[i]["utilityName"] = "MCE Clean Energy"
        if not rate_data[i].get("sourceReference", None):
            rate_data[i]["sourceReference"] = SOURCE
        rate_data[i]["sector"] = "Commercial"
        rate_data[i]["effectiveDate"] = {
            "$date": int(datetime(2018, 4, 1, 0, 0).timestamp() * 1000)
        }

    with open(destination, "w") as fp:
        json.dump(rate_data, fp, indent=4, sort_keys=True)
