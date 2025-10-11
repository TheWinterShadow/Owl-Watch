import re
from typing import Dict

from execution.utils.logger import logger


def extract_partitions(s3_key: str) -> Dict[str, str]:
    logger.secure_info(f"Extracting partitions from S3 key: {s3_key}")

    partitions = {}

    partition_pattern = r"(\w+)=([^/]+)"
    matches = re.findall(partition_pattern, s3_key)

    for key, value in matches:
        partitions[key] = value

    logger.secure_info(f"Extracted partitions: {partitions}")

    return partitions
