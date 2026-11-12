from datetime import timedelta, datetime

def parse_duration(duration_str):
    """
    Description: Parses an ISO 8601 duration string and returns a timedelta object
    Input: str (ISO 8601 duration string, e.g., "PT1H30M45S")
    Output: timedelta (duration represented as a timedelta object)
    """

    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    total_duration = timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )
    return total_duration


def transform_data(row):
    """
    Description: Transforms a row of video data for database insertion
    Input: dict (row of video data)
    Output: dict (transformed row of video data)
    """

    duration_td = parse_duration(row["Duration"])
    row["Duration"] = (datetime.min + duration_td).time()
    row["Video_type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"
    
    # Ensure Comments_Count exists for database insertion
    if "Comments_count" not in row:
        row["Comments_count"] = row.get("commentCount", None)

    return row
