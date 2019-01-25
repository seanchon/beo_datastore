import enum


class Energy(enum.Enum):
    kWh = 1
    therm = 2


class UsageType(enum.Enum):
    Facility = 1
    Heating = 2
    HVAC = 3
    Fans = 4
    InteriorLights = 5
    ExteriorLights = 6
    InteriorEquipment = 7
    WaterHeater = 8
