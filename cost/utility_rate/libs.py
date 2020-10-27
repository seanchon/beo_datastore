from datetime import datetime
import pandas as pd


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
