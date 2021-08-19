"""
Module to define any extra settings and the transaction type and function
"""
from .transform import *


class LogMessages:
    """Data to change the log messages in the function"""

    initial: str = "Starting to process data for [{msg}]"
    no_command_defined: str = "No command defined for the message: {msg}"
    finished_processing: str = "Finished processing data for {msg}."


files_definitions = {
    "mb52": {"type": "txt", "function": prepare_mb52},
    "mb51": {"type": "txt", "function": prepare_mb51},
    "zmm001": {"type": "txt", "function": prepare_zmm001},
    "zmb25": {"type": "txt", "function": prepare_zmb25},
    "picps": {"type": "txt", "function": prepare_materialclasses},
    "zfi": {"type": "xlsx", "function": prepare_zfi},
    "zmrp": {"type": "xlsx", "function": prepare_mrp},
    "mcba": {"type": "xlsx", "function": prepare_mcba},
    "monos_categories": {"type": "xlsx", "function": prepare_monos},
    "sp99": {"type": "csv", "function": prepare_sp99},
}
