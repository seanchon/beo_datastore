from datetime import datetime
import json


SOURCE = (
    "https://www.mcecleanenergy.org/wp-content/uploads/2019/12/"
    "MCE-residential-rates_070119.pdf"
)
EFFECTIVE_DATETIME = datetime(2019, 7, 1, 0, 0)

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
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 2, 2, 2, 2, 2],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 3, 3, 3, 3, 5, 5, 5, 5, 5],
]

# EV keys:
# 0 - Summer Peak, 1 - Summer Part-Peak, 2 - Summer Off-Peak,
# 3 - Winter Peak, 4, Winter Part-Peak, 5 Winter Off-Peak
EV_2_WEEKDAY_SCHED = [
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 4, 4, 4],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 4, 4, 4],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 4, 4, 4],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 4, 4, 4],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 4, 4, 4],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 0, 0, 0, 0, 0, 1, 1, 1],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 0, 0, 0, 0, 0, 1, 1, 1],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 0, 0, 0, 0, 0, 1, 1, 1],
    [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 0, 0, 0, 0, 0, 1, 1, 1],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 4, 4, 4],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 4, 4, 4],
    [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 3, 3, 3, 3, 4, 4, 4],
]
EV_2_WEEKEND_SCHED = EV_2_WEEKDAY_SCHED


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


def run(*args):
    """
    Usage:
        - python manage.py runscript cost.utility_rate.scripts.generate_residential_rates_mce --script-args DESTINATION
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
            [{"key": "All Electric Usage", "val": 0.087}],
            BASIC_SCHED,
            BASIC_SCHED,
        ),
        (
            "E-TOUA, Residential Time-of-Use",
            [
                {"key": "Summer Peak", "val": 0.170},
                {"key": "Summer Off-Peak", "val": 0.094},
                {"key": "Winter Peak", "val": 0.083},
                {"key": "Winter Off-Peak", "val": 0.069},
            ],
            TOU_A_WEEKDAY_SCHED,
            TOU_A_WEEKEND_SCHED,
        ),
        (
            "E-TOUB, Residential Time-of-Use",
            [
                {"key": "Summer Peak", "val": 0.191},
                {"key": "Summer Off-Peak", "val": 0.089},
                {"key": "Winter Peak", "val": 0.085},
                {"key": "Winter Off-Peak", "val": 0.066},
            ],
            TOU_B_WEEKDAY_SCHED,
            TOU_B_WEEKEND_SCHED,
        ),
        (
            "E-TOUC3, Residential Time-of-Use",
            [
                {"key": "Summer Peak", "val": 0.145},
                {"key": "Summer Off-Peak", "val": 0.082},
                {"key": "Winter Peak", "val": 0.089},
                {"key": "Winter Off-Peak", "val": 0.071},
            ],
            TOU_C_WEEKDAY_SCHED,
            TOU_C_WEEKEND_SCHED,
        ),
        (
            "E6, EM-TOU, Residential Time-of-Use",
            [
                {"key": "Summer Peak", "val": 0.223},
                {"key": "Summer Part-Peak", "val": 0.105},
                {"key": "Summer Off-Peak", "val": 0.057},
                {"key": "Winter Part-Peak", "val": 0.084},
                {"key": "Winter Off-Peak", "val": 0.071},
            ],
            EM_WEEKDAY_SCHED,
            EM_WEEKEND_SCHED,
        ),
        (
            "EV, Residential Rates for Electric Vehicle Owners",
            [
                {"key": "Summer Peak", "val": 0.247},
                {"key": "Summer Part-Peak", "val": 0.103},
                {"key": "Summer Off-Peak", "val": 0.037},
                {"key": "Winter Peak", "val": 0.073},
                {"key": "Winter Part-Peak", "val": 0.034},
                {"key": "Winter Off-Peak", "val": 0.039},
            ],
            EV_WEEKDAY_SCHED,
            EV_WEEKEND_SCHED,
        ),
        (
            "EV 2, Residential Rates for Electric Vehicle Owners",
            [
                {"key": "Summer Peak", "val": 0.154},
                {"key": "Summer Part-Peak", "val": 0.110},
                {"key": "Summer Off-Peak", "val": 0.069},
                {"key": "Winter Peak", "val": 0.098},
                {"key": "Winter Part-Peak", "val": 0.085},
                {"key": "Winter Off-Peak", "val": 0.062},
            ],
            EV_2_WEEKDAY_SCHED,
            EV_2_WEEKEND_SCHED,
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

    # add metadata
    for i, _ in enumerate(rate_data):
        rate_data[i]["approved"] = True
        rate_data[i]["utilityName"] = "MCE Clean Energy"
        if not rate_data[i].get("sourceReference", None):
            rate_data[i]["sourceReference"] = SOURCE
        rate_data[i]["sector"] = "Residential"
        rate_data[i]["effectiveDate"] = {
            "$date": int(EFFECTIVE_DATETIME.timestamp() * 1000)
        }

    with open(destination, "w") as fp:
        json.dump(rate_data, fp, indent=4, sort_keys=True)
