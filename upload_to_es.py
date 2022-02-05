import csv
import datetime
import pprint # debugging
from collections import deque

import pytz
from elasticsearch.helpers import parallel_bulk

from connection import local_client as client

mappings = {
    "pid": {"type": "keyword"},
    "stop_id": {"type": "keyword"},
    "exp_years": {"type": "integer"},
    "stop_datetime": {"type": "date"},
    "stop_duration_minutes": {"type": "integer"},
    "officer_assignment": {"type": "keyword"},
    "address_description": {"type": "text"},
    "perceived_age": {"type": "integer"},
    "perceived_gender": {"type": "keyword"},
    "driver": {"type": "boolean"},
    "response_to_service_call": {"type": "boolean"},
    "perceived_lgbtqia": {"type": "boolean"},
    "perceived_transgender": {"type": "boolean"},
    "perceived_limited_english": {"type": "boolean"},
    "beat": {"type": "keyword"},
    "city": {"type": "keyword"},
}


def parse_gender_lgbtqia(
    gender: str,
    gender_nonconforming: bool,
    lgbtqia: bool,
) -> dict[str:str]:
    """Logic to clean up percieved gender identity"""
    transgender = "trans" in gender.lower()

    # Determine 'Male/Female/Non-Binary'
    if gender not in ["Male", "Female"]:
        lgbtqia = True

        # remove inappropriate non-affirmation of trans-identities
        # CA state law identifies three genders (Male, Female, Non-Binary)
        if "boy" in gender.lower():
            gender = "Male"

        elif "girl" in gender.lower():
            gender = "Female"

        if not gender and gender_nonconforming:
            gender = "Non-Binary"

    return {
        "perceived_gender": gender,
        "perceived_transgender": transgender,
        "perceived_lgbtqia": lgbtqia,
    }


def parse_date(
    date: str, time: str, tz_location: str = "America/Los_Angeles"
) -> datetime.datetime:
    """
    Combines the date and time into a datetime object with timezone awareness

    >>> parse_date('2022-01-25', '10:10:05')
    datetime.datetime(2022, 1, 25, 10, 10, 5, tzinfo=<DstTzInfo 'America/Los_Angeles' LMT-1 day, 16:07:00 STD>)
    """
    date = datetime.date.fromisoformat(date)
    time = datetime.time.fromisoformat(time)
    tz = pytz.timezone(tz_location)
    return datetime.datetime.combine(date, time, tzinfo=tz)


def parse_address(address_fields: dict) -> str:
    """create an address description based on the following:
    intersection, land_mark, address_block, address_street, highway exit,
    school_name
    """

    # address_block is a float (400.0) and needs to just be a string ("400")
    address_block = address_fields.get("address_block", None)

    if address_block:
        address_block = str(int(float(address_block)))

    # combine landmarks into descriptors with address.
    landmark_entries = [
        address_fields.get("land_mark", None),
        address_fields.get("school_name", None),
    ]
    landmark = " ".join([x for x in landmark_entries if x])
    address_street_entries = [
        address_block,
        address_fields.get("address_street", None),
    ]
    address = " ".join([x for x in address_street_entries if x])

    if landmark and address:
        address = f"{landmark} on {address}"
        landmark = ""  # clear out to avoide duplication issues

    # simplify the request
    primary_road_components = [
        landmark,
        address,
        address_fields.get("highway_exit", None),
    ]
    address = " ".join([x for x in primary_road_components if x])

    if (intersection:=address_fields["intersection"]) and not address:
        return intersection

    elif intersection and address:
        return f"The intersection of {intersection} and {address}"

    else: 
        return address

def parse_truthiness(val: str, /) -> bool | str:
    """Convert all forms of truthiness to boolean"""

    match val.lower():
        case "yes" | "1" | "true" | "on":
            return True
        case "no" | "0" | "false" | "off":
            return False
        case _:
            return val


def parse_row(reader) -> None:
    """Parse CSV File as a file"""

    for row in reader:
        # pid will have often be 1 so do make id and driver before parse_truthiness
        boolean_entries = {"driver": row["pid"] == "1"}
        row["_id"] = row["stop_id"] + row["pid"]

        for k, v in mappings.items():
            if v["type"] == "boolean" and k in row:
                boolean_entries[k] = parse_truthiness(row.pop(k))

        address_fields = {
            "intersection": row.pop("intersection"),
            "address_block": row.pop("address_block"),
            "land_mark": row.pop("land_mark"),
            "address_street": row.pop("address_street"),
            "highway_exit": row.pop("highway_exit"),
            "school_name": row.pop("school_name"),
        }

        row["address_description"] = parse_address(address_fields)
        row["stop_datetime"] = parse_date(row.pop("date_stop"), row.pop("time_stop"))
        row["stop_duration_minutes"] = int(row.pop("stopduration"))
        row["response_to_service_call"] = row.pop("stop_in_response_to_cfs") == "1"
        row["officer_assignment"] = row.pop("assignment")
        row["city"] = row.pop("address_city")

        gender_nonconforming = any(
            [
                row.pop("gend_nc"),
                row.pop("gender_nonconforming"),
                row.pop("gend")
                == "5",  # the defined value for gender-nonconforming (1,2,5)
            ]
        )

        gender_perception = parse_gender_lgbtqia(
            gender=row['perceived_gender'],
            gender_nonconforming=gender_nonconforming,
            lgbtqia=row.pop("perceived_lgbt")=="1",
        )

        row = {**row, **boolean_entries, **gender_perception}

        keys_to_remove = [
            "ori",
            "agency",
            "isschool",
            "beat_name",
            "isstudent",
            "stopduration",
            "officer_assignment_key",
            "assignment",
        ]

        [row.pop(key, None) for key in keys_to_remove]
        yield row


def main(filename: str):
    """Main Runner for this script"""

    index="sd-ripa-ca0371100"
    client.indices.delete(index=index, ignore=[404])
    client.indices.create(index=index, mappings={"properties": mappings})

    with open(filename) as csv_file:
        reader = csv.DictReader(csv_file)
        deque(parallel_bulk(client, parse_row(reader), index=index))


if __name__ == "__main__":
    import doctest

    doctest.testmod()
    main("edited_ripa_stops_datasd.csv")
