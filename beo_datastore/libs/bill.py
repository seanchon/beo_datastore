from datetime import datetime
import attr
from functools import reduce
import pandas as pd
import re
import warnings

from beo_datastore.libs.dataframe import get_unique_values
from beo_datastore.libs.intervalframe import (
    ValidationDataFrame,
    ValidationFrame288,
    PowerIntervalFrame,
)
from beo_datastore.libs.units import DataUnitEnum, RateUnitEnum


SIMPLE_FIELDS = {
    "_id": lambda x: x["$oid"] if isinstance(x, dict) else {"$oid": x},
    "rateName": str,
    "sector": str,
    "sourceReference": str,
    "utilityName": str,
    "approved": bool,
    "effectiveDate": lambda x: datetime.fromtimestamp(
        int(int(x["$date"]) / 1000)
    )
    if isinstance(x, dict)
    else {"$date": int(pd.to_datetime(x).timestamp() * 1000)},
    "description": str,
    "fixedChargeFirstMeter": float,
    "fixedChargeUnits": str,
    "demandUnits": str,
    "demandRateUnits": str,
    "flatDemandUnits": str,
    "voltageCategory": str,
    "eiaId": int,
    "demandMin": int,
    "demandMax": int,
    "dgRules": str,
    "phaseWiring": str,
}
ENERGY_KEY_VALS = "energyKeyVals"
ENERGY_TABLE_FIELD = "energyRates"
DEMAND_TABLE_FIELD = "demandRates"
FLAT_DEMAND_TABLE_FIELD = "flatDemandRates"
JSON_TABLE_FIELDS = {
    ENERGY_TABLE_FIELD: {
        "strux": "energyRateStrux",
        "tier": "energyRateTiers",
        "default_unit": "kWh",
    },
    DEMAND_TABLE_FIELD: {
        "strux": "demandRateStrux",
        "tier": "demandRateTiers",
        "default_unit": "kW",
    },
    FLAT_DEMAND_TABLE_FIELD: {
        "strux": "flatDemandStrux",
        "tier": "flatDemandTiers",
        "default_unit": "kW",
    },
}
TOU_FIELDS = [
    "energyWeekdaySched",
    "energyWeekendSched",
    "demandWeekdaySched",
    "demandWeekendSched",
    "flatDemandMonths",
]


def convert_rate_df_to_dict(df):
    """
    Convert our NavigaDER templatized CSV format in a Pandas DataFrame
    object to a dictionary in the OpenEI format.
    """
    json_dict = {}
    skip = 0
    seen_periods = {key: set() for key in JSON_TABLE_FIELDS.keys()}
    for idx, row in df.iterrows():
        if skip > 0:
            skip -= 1
            continue
        field = row["field"]
        if field in SIMPLE_FIELDS:
            """
            Simple key value fields
            """
            value = SIMPLE_FIELDS[field](row[df.columns[1]])
            json_dict[field] = value
        elif field in JSON_TABLE_FIELDS:
            """
            An energy table field with [bucket, rate, unit, max, adj, sell]
            headers
            """
            strux = JSON_TABLE_FIELDS[field]["strux"]
            tier = JSON_TABLE_FIELDS[field]["tier"]
            if (
                field == ENERGY_TABLE_FIELD
                and ENERGY_KEY_VALS not in json_dict
            ):
                json_dict[ENERGY_KEY_VALS] = []
            if strux not in json_dict:
                json_dict[strux] = []
            assert (
                row[df.columns[1]] == "bucket"
                and row[df.columns[2]] == "rate"
                and row[df.columns[3]] == "unit"
            )
            i = 1
            while str(df.loc[idx + i, "field"]).isnumeric():
                bucket = df.loc[idx + i, "0"]
                bucket = "" if pd.isna(bucket) else bucket
                period = int(df.loc[idx + i, "field"])
                tier_dict = {
                    "rate": float(df.loc[idx + i, "1"]),
                    "unit": df.loc[idx + i, "2"],
                }
                maximum = float(df.loc[idx + i, "3"])
                adj = float(df.loc[idx + i, "4"])
                sell = float(df.loc[idx + i, "5"])
                if not pd.isna(maximum):
                    tier_dict["max"] = maximum
                if not pd.isna(adj):
                    tier_dict["adj"] = adj
                if not pd.isna(sell):
                    tier_dict["sell"] = sell
                if period in seen_periods[field]:
                    json_dict[strux][period - 1][tier].append(tier_dict)
                else:
                    if field == ENERGY_TABLE_FIELD:
                        json_dict[ENERGY_KEY_VALS].append(
                            {"key": bucket, "val": period}
                        )
                    json_dict[strux].append({tier: [tier_dict]})
                seen_periods[field].add(period)
                i += 1
                skip += 1
        elif field in TOU_FIELDS:
            """
            A 288 frame dictating the TOU plan, these values must be
            decremented by 1 to match the JSON
            """
            skip = 1 if field[:4] == "flat" else 24
            grid = df.loc[idx + 1 : idx + skip]
            json_dict[field] = []
            for i in range(0, 12):
                lst = list(grid[str(i)].astype(int) - 1)
                if skip == 1:
                    lst = lst[0]
                json_dict[field].append(lst)
    return json_dict


def convert_rate_dict_to_df(json_dict):
    """
    Convert a dictionary in OpenEI format for rate data into a Pandas DataFrame
    object in our NavigaDER templatized CSV format.
    """
    df = pd.DataFrame(columns=["field"])
    for field in SIMPLE_FIELDS.keys():
        val = json_dict.get(field, None)
        if val:
            val = SIMPLE_FIELDS[field](val)
            series = {"field": field, 0: val}
            df = df.append(series, ignore_index=True)
    for key, json_field in JSON_TABLE_FIELDS.items():
        strux = json_dict.get(json_field["strux"], None)
        if strux is not None:
            field_header = {
                "field": key,
                0: "bucket",
                1: "rate",
                2: "unit",
                3: "max",
                4: "adj",
                5: "sell",
            }
            df = df.append(field_header, ignore_index=True)
            key_vals = json_dict.get(ENERGY_KEY_VALS, None)
            for i in range(len(strux)):
                field = strux[i]
                for tier in field[json_field["tier"]]:
                    series = {
                        "field": i + 1,
                        0: key_vals[i]["key"] if key_vals is not None else "",
                        1: tier.get("rate", ""),
                        2: tier.get("unit", json_field["default_unit"]),
                        3: tier.get("max", ""),
                        4: tier.get("adj", ""),
                        5: tier.get("sell", ""),
                    }
                    df = df.append(series, ignore_index=True)
    months = [datetime(2000, i, 1).strftime("%b") for i in range(1, 13)]
    for tou_field in TOU_FIELDS:
        val = json_dict.get(tou_field, None)
        if val is not None:
            flat = "flat" == tou_field[:4]
            header = {i: m for i, m in enumerate(months)}
            header["field"] = tou_field
            df = df.append(header, ignore_index=True)
            body = pd.DataFrame(
                val, columns=[i for i in range(1 if flat else 24)]
            )
            body += 1
            body = body.transpose()
            body["field"] = body.index.to_series()
            df = df.append(body, ignore_index=True, sort=False)
    return df


@attr.s(frozen=True)
class OpenEIRateData(object):
    """
    Container class for extracting fixed, energy, and demand charges from
    OpenEI's U.S. Utility Rate Database (JSON data).

    Homepage: https://openei.org/wiki/Utility_Rate_Database
    JSON data: https://openei.org/apps/USURDB/download/usurdb.json.gz
    """

    rate_data = attr.ib(type=dict, repr=False)

    @property
    def fixed_rate_keys(self):
        """
        Return set of fixed-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "fixed" in x.lower()}

    @property
    def fixed_rates(self):
        """
        Return fixed rates applied on a per day, per month, etc. basis.
        """
        return self.rate_data.get("fixedKeyVals", [])

    @property
    def fixed_rate_unit(self):
        """
        Return fixed rate unit (ex. $/day, $/month).
        """
        fixed_charge_units = self.rate_data.get("fixedChargeUnits", None)

        # defaults to $/day
        if fixed_charge_units is None:
            return RateUnitEnum(
                numerator=DataUnitEnum.get_enum(alias="$"),
                denominator=DataUnitEnum.get_enum(alias="day"),
            )
        else:
            numerator_alias = fixed_charge_units.split("/")[0]
            denominator_alias = fixed_charge_units.split("/")[1]
            return RateUnitEnum(
                numerator=DataUnitEnum.get_enum(alias=numerator_alias),
                denominator=DataUnitEnum.get_enum(alias=denominator_alias),
            )

    @property
    def fixed_meter_charge(self):
        """
        Return fixed meter charge per bill ($/month).
        """
        return self.rate_data.get("fixedChargeFirstMeter", 0)

    @property
    def energy_rate_keys(self):
        """
        Return set of energy-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "energy" in x.lower()}

    @property
    def energy_rates(self):
        """
        Return energy rates applied on a per kWh basis.
        """
        return self.rate_data.get("energyRateStrux", [])

    @property
    def energy_rate_unit(self):
        """
        RateUnitEnum for energy rates.
        """
        return RateUnitEnum(
            numerator=DataUnitEnum.get_enum(alias="$"),
            denominator=DataUnitEnum.get_enum(alias="kwh"),
        )

    @property
    def energy_weekday_schedule(self):
        """
        Return the weekday schedule for energy charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("energyWeekdaySched")

    @property
    def energy_weekend_schedule(self):
        """
        Return the weekend schedule for energy charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("energyWeekendSched")

    @property
    def demand_rate_keys(self):
        """
        Return set of demand-rate keys found in self.rate_data.
        """
        return {x for x in self.rate_data.keys() if "demand" in x.lower()}

    @property
    def demand_min(self):
        """
        Minimum demand allowable for collection of rates.
        """
        return self.rate_data.get("demandMin", 0)

    @property
    def demand_max(self):
        """
        Maximum demand allowable for collection of rates.
        """
        return self.rate_data.get("demandMax", float("inf"))

    @property
    def demand_rates(self):
        """
        TOU-based demand rates.
        """
        return self.rate_data.get("demandRateStrux", [])

    @property
    def demand_rate_unit(self):
        """
        RateUnitEnum for TOU-based demand rates.
        """
        demand_rate_units = self.rate_data.get("demandRateUnits", None)

        # defaults to $/kW
        if demand_rate_units is None:
            return RateUnitEnum(
                numerator=DataUnitEnum.get_enum(alias="$"),
                denominator=DataUnitEnum.get_enum(alias="kW"),
            )
        else:
            return RateUnitEnum(
                numerator=DataUnitEnum.get_enum(alias="$"),
                denominator=DataUnitEnum.get_enum(alias=demand_rate_units),
            )

    @property
    def demand_weekday_schedule(self):
        """
        Return the weekday schedule for demand charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("demandWeekdaySched")

    @property
    def demand_weekend_schedule(self):
        """
        Return the weekend schedule for demand charges.

        :return: ValidationFrame288
        """
        return self.get_tou_schedule("demandWeekendSched")

    @property
    def flat_demand_rates(self):
        """
        Month-based (seasonal) demand rates.
        """
        return self.rate_data.get("flatDemandStrux", [])

    @property
    def flat_demand_rate_unit(self):
        """
        RateUnitEnum for month-based (seasonal) demand rates.
        """
        flat_demand_units = self.rate_data.get("flatDemandUnits", None)

        # defaults to $/kW
        if flat_demand_units is None:
            return RateUnitEnum(
                numerator=DataUnitEnum.get_enum(alias="$"),
                denominator=DataUnitEnum.get_enum(alias="kW"),
            )
        else:
            return RateUnitEnum(
                numerator=DataUnitEnum.get_enum(alias="$"),
                denominator=DataUnitEnum.get_enum(alias=flat_demand_units),
            )

    @property
    def flat_demand_schedule(self):
        """
        Return ValidationFrame288 representation of month-based (seasonal)
        demand schedule.
        """
        return ValidationFrame288.convert_matrix_to_frame288(
            [
                [x] * 24
                for x in self.rate_data.get("flatDemandMonths", [None] * 12)
            ]
        )

    def get_tou_schedule(self, lookup_key):
        """
        Return a 12 x 24 lookup table of TOU demand or energy schedules.

        :param lookup_key: choice of "demandWeekdaySched",
            "demandWeekendSched", "energyWeekdaySched", "energyWeekendSched"
        :return: ValidationFrame288 object
        """
        if lookup_key not in [
            "demandWeekdaySched",
            "demandWeekendSched",
            "energyWeekdaySched",
            "energyWeekendSched",
        ]:
            raise KeyError(
                "Choices are demandWeekdaySched, demandWeekendSched, "
                "energyWeekdaySched, energyWeekendSched."
            )

        tou_matrix = self.rate_data.get(lookup_key, None)

        return ValidationFrame288.convert_matrix_to_frame288(tou_matrix)

    def get_energy_rate(self, tou_id, tier):
        """
        Return rate value from self.energy_rates at tou_id and tier.
        """
        try:
            return self.energy_rates[tou_id]["energyRateTiers"][tier]["rate"]
        except (IndexError, TypeError):
            return None

    def get_demand_rate(self, tou_id, tier):
        """
        Return rate value from self.demand_rates at tou_id and tier.
        """
        try:
            return self.demand_rates[tou_id]["demandRateTiers"][tier]["rate"]
        except (IndexError, TypeError):
            return None

    def get_rate_frame288(self, rate_type, schedule_type, tier=0):
        """
        Return energy rates in ValidationFrame288 format.

        :param rate_type: choice "energy" or "demand"
        :param schedule_type: choice "weekday" or "weekend"
        :param tier: choice of tier for tiered-rates (integer)
        :return: ValidationFrame288
        """
        if rate_type == "energy":
            get_rate = self.get_energy_rate
            if schedule_type == "weekday":
                schedule = self.energy_weekday_schedule
            elif schedule_type == "weekend":
                schedule = self.energy_weekend_schedule
            else:
                raise AttributeError(
                    "schedule_type options are weekday or weekend"
                )
        elif rate_type == "demand":
            get_rate = self.get_demand_rate
            if schedule_type == "weekday":
                schedule = self.demand_weekday_schedule
            elif schedule_type == "weekend":
                schedule = self.demand_weekend_schedule
            else:
                raise AttributeError(
                    "schedule_type options are weekday or weekend"
                )
        else:
            raise AttributeError("rate_type options are energy or demand")

        tou_ids = get_unique_values(schedule.dataframe)
        return ValidationFrame288(
            dataframe=schedule.dataframe.replace(
                tou_ids, [get_rate(tou_id=x, tier=tier) for x in tou_ids]
            )
        )


@attr.s()
class ValidationBill(ValidationDataFrame):
    """
    Container class for pandas DataFrames with the following columns:

        1. id
        2. category
        3. description
        4. count
        5. count_unit
        6. rate
        7. rate_unit
        8. pro_rata
        9. total
    """

    default_dataframe = pd.DataFrame(
        columns=[
            "category",
            "description",
            "tou_period",
            "count",
            "count_unit",
            "rate",
            "rate_unit",
            "pro_rata",
            "total",
        ]
    )

    intervalframe = attr.ib(type=PowerIntervalFrame)
    openei_rate_data = attr.ib(type=OpenEIRateData)

    @intervalframe.validator
    def validate_intervalframe(self, attribute, value):
        """
        Validate intervalframe does not contain too many days.
        """
        if value.days > 35:
            warning_message = (
                "intervalframe contains more than a month's worth of data "
                "({} days), which can create erroneous "
                "bills.".format(value.days)
            )
            warnings.warn(warning_message)

    def __attrs_post_init__(self):
        # TODO: change this so class can be frozen (immutable)
        self.dataframe = self.default_dataframe.copy()
        self.compute_bill()

    @property
    def start_datetime(self):
        """
        Return earliest timestamp as datetime object.
        """
        return self.intervalframe.start_datetime

    @property
    def end_datetime(self):
        """
        Return latest timestamp as datetime object.
        """
        return self.intervalframe.end_datetime

    @property
    def total(self):
        """
        Return total of all charges.
        """
        return self.dataframe["total"].sum()

    @property
    def total_dataframe(self):
        """
        Return self.dataframe with additional total line.
        """
        count_df = pd.DataFrame(
            self.dataframe["count"].sum(), index=["Total"], columns=["count"]
        )
        total_df = pd.DataFrame(
            self.dataframe["total"].sum(), index=["Total"], columns=["total"]
        )

        return self.dataframe.append(
            pd.merge(
                count_df,
                total_df,
                how="inner",
                left_index=True,
                right_index=True,
            ),
            sort=False,
        ).fillna("")

    @staticmethod
    def extract_rate(rate_string):
        """
        Return first found decimal in rate_string.

        :param rate_string: string
        :return: float
        """
        return float(re.findall(r"[-+]?\d*\.\d+|\d+", rate_string)[0])

    def compute_bill(self):
        """
        Compute bill only if self.dataframe is empty.
        """
        if self.dataframe.empty:
            self.compute_fixed_meter_charges()
            self.compute_fixed_rate_charges()
            self.compute_energy_rate_charges()
            self.compute_demand_rate_charges()
            self.compute_flat_demand_rate_charges()

    def add_charge(
        self,
        category,
        description,
        count,
        count_unit,
        rate,
        rate_unit,
        tou_period=None,
        pro_rata=1,
    ):
        """
        Add charge to ValidationBill.

        :param category: string
        :param description: string
        :param count: float
        :param count_unit: DataUnitEnum
        :param rate: float
        :param rate_unit: RateUnitEnum
        :param tou_period: string/int
        :param pro_rata: float proportion to prorate (ex. .80)
        """
        if (count_unit * rate_unit) != DataUnitEnum.DOLLAR:
            raise TypeError(
                "The DataUnitEnum multiplied by RateUnitEnum should yield a "
                "dollar amount."
            )

        self.dataframe = self.dataframe.append(
            {
                "category": category,
                "description": description,
                "tou_period": tou_period,
                "count": count,
                "count_unit": count_unit.print_alias,
                "rate": rate,
                "rate_unit": rate_unit.print_alias,
                "pro_rata": pro_rata,
                "total": count * rate * pro_rata,
            },
            ignore_index=True,
        )

    def compute_fixed_meter_charges(self):
        """
        Add fixed meter charge to bill if applicable.
        """
        if self.openei_rate_data.fixed_meter_charge:
            self.add_charge(
                category="fixed",
                description="Fixed Charge First Meter",
                count=1,
                count_unit=DataUnitEnum.get_enum(alias="month"),
                rate=self.openei_rate_data.fixed_meter_charge,
                rate_unit=RateUnitEnum(
                    numerator=DataUnitEnum.get_enum(alias="$"),
                    denominator=DataUnitEnum.get_enum(alias="month"),
                ),
            )

    def compute_fixed_rate_charges(self):
        """
        Extract fixed rates from self.openei_rate_data and fixed counts from
        self.intervalframe to compute fixed charges.
        """
        # TODO: Remove optional fixed charges/credits
        for rate in self.openei_rate_data.fixed_rates:
            if "/day" in rate.get("key"):
                rate_unit = RateUnitEnum(
                    numerator=DataUnitEnum.get_enum(alias="$"),
                    denominator=DataUnitEnum.get_enum(alias="day"),
                )
            else:
                rate_unit = self.openei_rate_data.fixed_rate_unit
            period = rate_unit.denominator

            if period.name == "month":
                count = 1
            elif period.name == "day":
                count = self.intervalframe.days
            else:
                raise LookupError(
                    "Period {} not expected for fixed charge.".format(period)
                )

            self.add_charge(
                category="fixed",
                description=rate.get("key"),
                count=count,
                count_unit=period,
                rate=self.extract_rate(rate.get("val")),
                rate_unit=rate_unit,
            )

    @staticmethod
    def get_energy_count(
        intervalframe, weekday_schedule, weekend_schedule, tou_key
    ):
        """
        Return energy counts (kWh) based off of a tou_key.

        :param intervalframe: PowerIntervalFrame
        :param weekday_schedule: ValidationFrame288
        :param weekend_schedule: ValidationFrame288
        :param tou_key: int
        :return: float
        """
        weekday_totals = intervalframe.filter_by_weekday().total_frame288
        filtered_weekday_totals = weekday_totals * weekday_schedule.get_mask(
            tou_key
        )

        weekend_totals = intervalframe.filter_by_weekend().total_frame288
        filtered_weekend_totals = weekend_totals * weekend_schedule.get_mask(
            tou_key
        )

        return (
            (filtered_weekday_totals + filtered_weekend_totals)
            .dataframe.sum()
            .sum()
        )

    def get_billing_energy_count(self, tou_key):
        """
        Return billing energy counts (kWh) based off of a tou_key.

        :param tou_key: int
        :return: float
        """
        return self.get_energy_count(
            self.intervalframe,
            self.openei_rate_data.energy_weekday_schedule,
            self.openei_rate_data.energy_weekend_schedule,
            tou_key,
        )

    def get_energy_tou_key_value(self, tou_key):
        """
        Return TOU description if it exists. (ex. 'TOU-winter:Off-Peak')
        """
        tou_key_values = self.openei_rate_data.rate_data.get(
            "energyKeyVals", []
        )

        if not tou_key_values:
            return ""
        else:
            return tou_key_values[tou_key].get("key", "")

    def compute_energy_rate_charges(self):
        """
        Extract energy rates from self.openei_rate_data and energy counts from
        self.intervalframe to compute energy charges.
        """
        for tou_key, rates in enumerate(self.openei_rate_data.energy_rates):
            # initialize counts
            energy_count = self.get_billing_energy_count(tou_key)
            billed_so_far = 0

            for tier in rates.get("energyRateTiers", []):
                # 1. extract rate data
                rate = tier.get("rate", 0)
                if rate == 0:
                    warnings.warn("Rate missing. 0 $/kwh used instead.")
                max_kwh_per_day = tier.get("max", float("inf"))
                max_kwh_per_bill = max_kwh_per_day * self.intervalframe.days

                # 2. calculate energy usage per tier
                if energy_count <= 0:
                    if billed_so_far == 0 and energy_count < 0:
                        # bill all net production in first tier
                        # TODO: are NEM export bills tiered?
                        billing_count = energy_count
                    else:
                        # nothing left to bill
                        break
                else:
                    if (energy_count + billed_so_far) >= max_kwh_per_bill:
                        # bill only to max kWh per tier
                        billing_count = max_kwh_per_bill - billed_so_far
                    else:
                        # bill remaining energy count
                        billing_count = energy_count

                # 3. apply charge
                description = "Energy Charge"
                if self.get_energy_tou_key_value(tou_key):
                    description += " - {}".format(
                        self.get_energy_tou_key_value(tou_key)
                    )
                if max_kwh_per_day != float("inf"):
                    description += " ({} max kWh/day)".format(max_kwh_per_day)

                self.add_charge(
                    category="energy",
                    description=description,
                    count=billing_count,
                    count_unit=DataUnitEnum.get_enum(alias="kwh"),
                    rate=rate,
                    rate_unit=self.openei_rate_data.energy_rate_unit,
                    tou_period=tou_key,
                )

                # 4. update counts
                energy_count = energy_count - billing_count
                billed_so_far = billed_so_far + billing_count

    @staticmethod
    def get_demand_peak(
        intervalframe, weekday_schedule, weekend_schedule, tou_key
    ):
        """
        Return TOU-based demand peak power (kW) based off of a tou_key.

        :param tou_key: int
        :return: float
        """
        weekday_peaks = intervalframe.filter_by_weekday().maximum_frame288
        weekend_peaks = intervalframe.filter_by_weekend().maximum_frame288

        weekday_max = (
            (weekday_peaks * weekday_schedule.get_mask(tou_key))
            .dataframe.max()
            .max()
        )
        weekend_max = (
            (weekend_peaks * weekend_schedule.get_mask(tou_key))
            .dataframe.max()
            .max()
        )

        return max(weekday_max, weekend_max)

    def get_billing_demand_peak(self, tou_key):
        """
        Return billing TOU-based demand peak power (kW) based off of a tou_key.

        :param tou_key: int
        :return: float
        """
        return self.get_demand_peak(
            self.intervalframe,
            self.openei_rate_data.demand_weekday_schedule,
            self.openei_rate_data.demand_weekend_schedule,
            tou_key,
        )

    def get_demand_days(self, tou_key):
        """
        Return the number of billing days matching tou_key for prorating
        TOU-based demand charges.

        :param tou_key: int
        :return: int
        """
        weekday_schedule = self.openei_rate_data.demand_weekday_schedule
        months_1 = weekday_schedule.get_mask(tou_key).dataframe.any()
        months_1 = months_1.index[months_1 == 1].to_list()

        weekend_schedule = self.openei_rate_data.demand_weekend_schedule
        months_2 = weekend_schedule.get_mask(tou_key).dataframe.any()
        months_2 = months_2.index[months_2 == 1].to_list()

        return self.intervalframe.filter_by_months(
            months=set(months_1 + months_2)
        ).days

    def compute_demand_rate_charges(self):
        """
        Extract demand rates from self.openei_rate_data and demand peaks from
        self.intervalframe to compute demand charges.
        """
        for tou_key, rates in enumerate(self.openei_rate_data.demand_rates):
            for tier in rates.get("demandRateTiers", []):
                # TODO: Are there tiered demand charges?
                rate = tier.get("rate", 0)
                max_demand_per_tier = tier.get("max", float("inf"))
                demand_peak = self.get_billing_demand_peak(tou_key)
                demand_days = self.get_demand_days(tou_key)
                if rate and demand_peak and demand_days:
                    description = "Demand Charge"
                    if demand_days != self.intervalframe.days:
                        description += " ({}/{} pro rata)".format(
                            demand_days, self.intervalframe.days
                        )
                    if max_demand_per_tier != float("inf"):
                        description += " ({} max kW/tier)".format(
                            max_demand_per_tier
                        )

                    self.add_charge(
                        category="demand",
                        description=description,
                        count=demand_peak,
                        count_unit=DataUnitEnum.get_enum(alias="kw"),
                        rate=rate,
                        rate_unit=self.openei_rate_data.demand_rate_unit,
                        tou_period=tou_key,
                        pro_rata=(demand_days / self.intervalframe.days),
                    )

    @staticmethod
    def get_flat_demand_peak(intervalframe, schedule, tou_key):
        """
        Return flat-demand peak power (kW) based off of a tou_key.

        :param intervalframe: PowerIntervalFrame
        :param schedule: array of ints
        :param tou_key: int
        :return: float
        """
        peaks = intervalframe.maximum_frame288
        return (peaks * schedule.get_mask(tou_key)).dataframe.max().max()

    def get_billing_flat_demand_peak(self, tou_key):
        """
        Return billing flat-demand peak power (kW) based off of a tou_key.

        :param tou_key: int
        :return: float
        """
        return self.get_flat_demand_peak(
            self.intervalframe,
            self.openei_rate_data.flat_demand_schedule,
            tou_key,
        )

    def get_flat_demand_days(self, tou_key):
        """
        Return the number of billing days matching tou_key for prorating flat
        demand charges.

        :param tou_key: int
        :return: int
        """
        schedule = self.openei_rate_data.flat_demand_schedule
        months = schedule.get_mask(tou_key).dataframe.any()
        months = months.index[months == 1].to_list()

        return self.intervalframe.filter_by_months(months=months).days

    def compute_flat_demand_rate_charges(self):
        """
        Extract flat demand rates from self.openei_rate_data and demand peaks
        from self.intervalframe to compute flat demand charges.
        """
        for tou_key, rates in enumerate(
            self.openei_rate_data.flat_demand_rates
        ):
            for tier in rates.get("flatDemandTiers", []):
                # TODO: Are there tiered demand charges?
                rate = tier.get("rate", 0)
                max_demand_per_tier = tier.get("max", float("inf"))
                demand_peak = self.get_billing_flat_demand_peak(tou_key)
                demand_days = self.get_flat_demand_days(tou_key)
                if rate and demand_peak and demand_days:
                    description = "Flat Demand Charge"
                    if demand_days != self.intervalframe.days:
                        description += " ({}/{} pro rata)".format(
                            demand_days, self.intervalframe.days
                        )
                    if max_demand_per_tier != float("inf"):
                        description += " ({} max kW/tier)".format(
                            max_demand_per_tier
                        )

                    rate_unit = self.openei_rate_data.flat_demand_rate_unit
                    self.add_charge(
                        category="demand",
                        description=description,
                        count=demand_peak,
                        count_unit=DataUnitEnum.get_enum(alias="kw"),
                        rate=rate,
                        rate_unit=rate_unit,
                        tou_period=tou_key,
                        pro_rata=(demand_days / self.intervalframe.days),
                    )


class BillingCollection(object):
    """
    Container class for a collection of one or more ValidationBills.
    """

    def __init__(self, bills):
        """
        Initialize with a collection of ValidationBills.

        :param bills: list of ValidationBills
        """
        self.validate_bills(bills)
        self.bills = bills

    @property
    def start_datetime(self):
        """
        Return earliest timestamp as datetime object.
        """
        return self.intervalframe.start_datetime

    @property
    def end_datetime(self):
        """
        Return latest timestamp as datetime object.
        """
        return self.intervalframe.end_datetime

    @property
    def total(self):
        """
        Return total of all bill totals.
        """
        return reduce(lambda x, y: x + y, [x.total for x in self.bills])

    @property
    def dataframe(self):
        """
        Return billing dataframe representing all bills.
        """
        return reduce(
            lambda x, y: x.append(y),
            [x.dataframe for x in self.bills],
            pd.DataFrame(),
        )

    @property
    def total_dataframe(self):
        """
        Return self.dataframe with additional total line.
        """
        count_df = pd.DataFrame(
            self.dataframe["count"].sum(), index=["Total"], columns=["count"]
        )
        total_df = pd.DataFrame(
            self.dataframe["total"].sum(), index=["Total"], columns=["total"]
        )

        return self.dataframe.append(
            pd.merge(
                count_df,
                total_df,
                how="inner",
                left_index=True,
                right_index=True,
            ),
            sort=False,
        ).fillna("")

    @property
    def intervalframe(self):
        """
        Return PowerIntervalFrame representing meter readings from all bills.
        """
        return reduce(
            lambda x, y: x.merge_intervalframe(y),
            [x.intervalframe for x in self.bills],
            PowerIntervalFrame(),
        )

    @property
    def openei_rate_dict(self):
        """
        Return list of OpenEIRateData dicts representing all bills.
        """
        return [x.openei_rate_data.rate_data for x in self.bills]

    @staticmethod
    def validate_bills(bills):
        """
        Validates all bills are ValidationBills.
        """
        for bill in bills:
            if not isinstance(bill, ValidationBill):
                raise TypeError("{} must be a ValidationBill".format(bill))
