from datetime import datetime
import json


SOURCE = (
    "https://www.mcecleanenergy.org/wp-content/uploads/2017/03/"
    "MCE_Residential_Rates_Apr2017.pdf"
)

BASIC_SCHED = [[0] * 24] * 12

# TOU A keys:
# 0 - Summer Peak, 1 - Summer Off-Peak, 2 - Winter Peak, 3 - Winter Off-Peak
TOU_A_WEEKDAY_SCHED = [
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3, 3],
]
TOU_A_WEEKEND_SCHED = [
    [3] * 24,
    [3] * 24,
    [3] * 24,
    [3] * 24,
    [3] * 24,
    [1] * 24,
    [1] * 24,
    [1] * 24,
    [1] * 24,
    [3] * 24,
    [3] * 24,
    [3] * 24,
]

# TOU B keys:
# 0 - Summer Peak, 1 - Summer Off-Peak, 2 - Winter Peak, 3 - Winter Off-Peak
TOU_B_WEEKDAY_SCHED = [
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3],
    [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 3, 3, 3],
]
TOU_B_WEEKEND_SCHED = TOU_A_WEEKEND_SCHED

# TOU C keys:
# 0 - Summer Peak, 1 - Summer Off-Peak, 2 - Winter Peak, 3 - Winter Off-Peak
TOU_C_WEEKDAY_SCHED = TOU_B_WEEKDAY_SCHED
TOU_C_WEEKEND_SCHED = TOU_C_WEEKDAY_SCHED

# EM TOU keys:
# 0 - Summer Peak, 1 - Summer Part-Peak, 2 - Summer Off-Peak,
# 3, Winter Part-Peak, 4 Winter Off-Peak
EM_WEEKDAY_SCHED = [
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
]
EM_WEEKEND_SCHED = [
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
    [4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 4, 4, 4, 4],
]

# EV keys:
# 0 - Summer Peak, 1 - Summer Part-Peak, 2 - Summer Off-Peak,
# 3 - Winter Peak, 4, Winter Part-Peak, 5 Winter Off-Peak
EV_WEEKDAY_SCHED = [
    [5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 4, 4, 5],
    [5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 4, 4, 5],
    [5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 4, 4, 5],
    [5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 4, 4, 5],
    [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2],
    [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2],
    [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2],
    [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2],
    [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2],
    [2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2],
    [5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 4, 4, 5],
    [5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 4, 4, 5],
]
EV_WEEKEND_SCHED = [
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
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
    }
    return {x: y for x, y in demand_dict.items() if y is not None}


def run(*args):
    """
    Usage:
        - python manage.py runscript cost.utility_rate.scripts.generate_mce_residential_rates --script-args DESTINATION
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
    for name, rate_list, weekday_sched, weekend_sched in [
        (
            "E1, EM, ES, ESR, ET, Basic Residential",
            [{"key": "All Electric Usage", "val": 0.068}],
            BASIC_SCHED,
            BASIC_SCHED,
        ),
        (
            "E-TOUA, Residential Time-of-Use",
            [
                {"key": "Summer Peak", "val": 0.153},
                {"key": "Summer Off-Peak", "val": 0.078},
                {"key": "Winter Peak", "val": 0.066},
                {"key": "Winter Off-Peak", "val": 0.052},
            ],
            TOU_A_WEEKDAY_SCHED,
            TOU_A_WEEKEND_SCHED,
        ),
        (
            "E-TOUB, Residential Time-of-Use",
            [
                {"key": "Summer Peak", "val": 0.178},
                {"key": "Summer Off-Peak", "val": 0.072},
                {"key": "Winter Peak", "val": 0.069},
                {"key": "Winter Off-Peak", "val": 0.049},
            ],
            TOU_B_WEEKDAY_SCHED,
            TOU_B_WEEKEND_SCHED,
        ),
        (
            "E6, EM-TOU, Residential Time-of-Use",
            [
                {"key": "Summer Peak", "val": 0.186},
                {"key": "Summer Part-Peak", "val": 0.082},
                {"key": "Summer Off-Peak", "val": 0.043},
                {"key": "Winter Part-Peak", "val": 0.065},
                {"key": "Winter Off-Peak", "val": 0.052},
            ],
            EM_WEEKDAY_SCHED,
            EM_WEEKEND_SCHED,
        ),
        (
            "EV, Residential Rates for Electric Vehicle Owners",
            [
                {"key": "Summer Peak", "val": 0.200},
                {"key": "Summer Part-Peak", "val": 0.075},
                {"key": "Summer Off-Peak", "val": 0.030},
                {"key": "Winter Peak", "val": 0.055},
                {"key": "Winter Part-Peak", "val": 0.030},
                {"key": "Winter Off-Peak", "val": 0.030},
            ],
            EV_WEEKDAY_SCHED,
            EV_WEEKEND_SCHED,
        ),
    ]:
        energy_key_vals = []
        energy_rate_strux = []
        for i, rate in enumerate(rate_list):
            # add PCIA and Franchise Fee
            energy_key_vals.append({"key": rate["key"], "val": i + 1})
            energy_rate_strux.append(
                {
                    "energyRateTiers": [
                        {"unit": "kWh", "rate": rate_list[i]["val"]}
                    ]
                }
            )

        energy_dict = create_energy_dict(
            energy_key_vals=energy_key_vals,
            energy_rate_strux=energy_rate_strux,
            energy_weekday_schedule=weekday_sched,
            energy_weekend_schedule=weekend_sched,
        )

        energy_dict["rateName"] = name

        rate_data.append(energy_dict)

    # Create Deep Green Rates
    energy_key_vals = [{"key": "Deep Green $0.01/kWh", "val": 1}]
    energy_rate_strux = [{"energyRateTiers": [{"unit": "kWh", "rate": 0.01}]}]
    energy_dict = create_energy_dict(
        energy_key_vals=energy_key_vals,
        energy_rate_strux=energy_rate_strux,
        energy_weekday_schedule=BASIC_SCHED,
        energy_weekend_schedule=BASIC_SCHED,
    )

    energy_dict["rateName"] = "Deep Green (Residential)"

    rate_data.append(energy_dict)

    # add metadata
    for i, _ in enumerate(rate_data):
        rate_data[i]["approved"] = True
        rate_data[i]["utilityName"] = "MCE Clean Energy"
        rate_data[i]["sourceReference"] = SOURCE
        rate_data[i]["sector"] = "Residential"
        rate_data[i]["effectiveDate"] = {
            "$date": int(datetime(2017, 4, 1, 0, 0).timestamp() * 1000)
        }

    with open(destination, 'w') as fp:
        json.dump(rate_data, fp, sort_keys=True)
