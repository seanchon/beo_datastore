from bs4 import BeautifulSoup
import re
import requests

from django.core.exceptions import ObjectDoesNotExist
from django.core.management import call_command

from load.openei.models import ReferenceBuilding
from reference.reference_unit.models import BuildingType, DataUnit


COMMERCIAL_LOAD_DATA = (
    "https://openei.org/datasets/files/961/pub"
    "/COMMERCIAL_LOAD_DATA_E_PLUS_OUTPUT/"
)


def load_reference_units():
    call_command("loaddata", "reference_unit")


def get_links(url, filter_string):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    links = (x.get("href") for x in soup.find_all("a"))
    links = [x for x in links if filter_string in x]

    return links


def get_commercial_load_data_directories(url):
    """
    Returns all directory links (CA only) from:

    https://openei.org/datasets/files/961/pub
    /COMMERCIAL_LOAD_DATA_E_PLUS_OUTPUT/
    """
    return [(url + x) for x in get_links(url, "USA_CA")]


def get_all_commercial_load_data_links(url):
    """
    Returns all .csv files located in sub directories.
    """
    links = []
    for base_dir in get_commercial_load_data_directories(url):
        links += [(base_dir + x) for x in get_links(base_dir, ".csv")]

    return links


def parse_building_type(file_name):
    building_type = file_name.split("RefBldg")[-1].split("New2004")[0]
    building_type = re.sub(r"(\w)([A-Z])", r"\1 \2", building_type)

    if building_type == "Out Patient":
        return BuildingType.objects.get(name="Outpatient Health Care")
    else:
        return BuildingType.objects.get(name=building_type)


def parse_location(dir_name):
    location = re.split("USA_\w{2}_", dir_name)[-1]
    location = re.split("\.\d{6}_TMY3", location)[0]
    location = " ".join(location.split(".")).title()

    for str_1, str_2 in [
        ("Awos", "AWOS"),
        ("Ap", "AP"),
        ("Afb", "AFB"),
        ("Mcas", "MCAS"),
        ("Cgas", "CGAS"),
        ("Naf", "NAF"),
        ("Nas", "NAS"),
    ]:
        location = location.replace(str_1, str_2)

    return location.strip()


def parse_tmy3(dir_name):
    result = re.search("\d{6}", dir_name)
    if result:
        return result[0]
    else:
        return None


def parse_building_attributes(csv_url):
    dir_name = csv_url.split("/")[-2]
    file_name = csv_url.split("/")[-1]

    try:
        building_type = parse_building_type(file_name)
        location = parse_location(dir_name)
        tmy3 = parse_tmy3(dir_name)
        return (csv_url, building_type, location, tmy3)
    except ObjectDoesNotExist:
        print("ERROR: {}".format(csv_url))
        return None


def run():
    load_reference_units()
    links = get_all_commercial_load_data_links(COMMERCIAL_LOAD_DATA)
    building_attrs = [parse_building_attributes(link) for link in links]
    for (csv_url, building_type, location, tmy3) in building_attrs:
        ReferenceBuilding.objects.get_or_create(
            location=location,
            state="CA",
            TMY3_id=tmy3,
            source_file_url=csv_url,
            data_unit=DataUnit.objects.get(name="kwh"),
            building_type=building_type,
        )
