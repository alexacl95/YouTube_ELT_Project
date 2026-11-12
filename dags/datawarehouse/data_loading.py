import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():
    """
    Description: Loads video data from a JSON file
    Input: None (reads data from a JSON file named YT_data_<current_date>.json)
    Output: list (video data loaded from the JSON file)
    """

    file_path = f"./data/YT_data_{date.today()}.json"

    try:
        logger.info(f"Processing file: YT_data_{date.today()}")

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)

        return data
    
    except FileNotFoundError:
        logger.error(f"file not found: {file_path}")
        raise
    
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise
