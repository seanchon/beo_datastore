import attr
from attr.validators import instance_of

from enum import Enum


class DataUnitEnum(Enum):
    DAY = "DAY"
    DOLLAR = "DOLLAR"
    KW = "KW"
    KWH = "KWH"
    MONTH = "MONTH"

    def __mul__(self, other):
        """
        Return resulting DataUnitEnum when a DataUnitEnum and RateUnitEnum are
        multiplied. Only multiplication returning a simple DataUnitEnum is
        allowed (ex. kWh * $/kWh = $).

        :param other: RateUnitEnum
        :return: DataUnitEnum
        """
        if not isinstance(other, RateUnitEnum):
            raise TypeError("{} must be of type RateUnitEnum.".format(other))
        elif self != other.denominator:
            raise ValueError(
                "Multiplying {} by {} would not return a DataUnitEnum".format(
                    self, other
                )
            )
        else:
            return other.numerator

    @property
    def print_alias(self):
        """
        Return preferred print alias (ex. $ instead of DOLLAR).
        """
        if self.value == "DAY":
            return "day"
        elif self.value == "DOLLAR":
            return "$"
        elif self.value == "KW":
            return "kW"
        elif self.value == "KWH":
            return "kWh"
        elif self.value == "MONTH":
            return "month"
        else:
            return self.value

    @classmethod
    def get_enum(cls, alias):
        """
        Return DataUnitEnum using alias.

        :param alias: string
        :return: DataUnitEnum
        """
        if alias == "$":
            return cls.DOLLAR
        elif hasattr(cls, alias.upper()):
            return getattr(cls, alias.upper())
        else:
            return getattr(cls, alias)


@attr.s(frozen=True)
class RateUnitEnum(object):
    """
    A RateUnitEnum is a composition of a numerator (DataUnitEnum) and a
    denominator (DataUnitEnum).
    """

    numerator = attr.ib(validator=instance_of(DataUnitEnum))
    denominator = attr.ib(validator=instance_of(DataUnitEnum))

    def __mul__(self, other):
        """
        Return resulting DataUnitEnum when a RateUnitEnum and DataUnitEnum are
        multiplied. Only multiplication returning a simple DataUnitEnum is
        allowed (ex. $/kWh * kWh = $).

        :param other: DataUnitEnum
        :return: DataUnitEnum
        """
        if not isinstance(other, DataUnitEnum):
            raise TypeError("{} must be of type DataUnitEnum.".format(other))
        elif self.denominator != other:
            raise ValueError(
                "Multiplying {} by {} would not return a DataUnitEnum".format(
                    self, other
                )
            )
        else:
            return self.numerator

    @property
    def print_alias(self):
        """
        Return preferred print alias (ex. $/kWh instead of DOLLAR/KWH).
        """
        return self.numerator.print_alias + "/" + self.denominator.print_alias
