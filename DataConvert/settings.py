from typing import List

from .transform import *

class Files:
    """Data class to hold the different messages allowed to be processed"""
    text_update: List[str] = ["MB51", "MB52", "ZMB25"]
    xlsx_folder: List[str] = ["MCBA"]
    xlsx: List[str] = ["ZFI"]
    other_text: List[str] = ["ZMM001-Extra", "MB51-MEP", "ZMM001", "MaterialClasses"]

    @classmethod
    def text(cls):
        return cls.text_update + cls.other_text

    @classmethod
    def all(cls):
        return cls.xlsx_folder + cls.xlsx + cls.text()

class LogMessages:
    """Data to change the log messages in the function"""

    initial: str = "Starting to process data for [{msg}]"
    csv_creation_success: str = "Success creating {msg}.csv!"
    no_command_defined: str = "No command defined for the message: {msg}"
    process_text: str = "Starting to process file {msg} as a text file"
    process_xlsx: str = "Starting to process file {msg} as an excel file"
    process_xlsx_folder: str = "Starting to process folder {msg}"
    specific_process: str = "Individual function processing has started"


    
starts = {      # change numbers here when needed
    "MB52": 1,
    "MB51": 22,
    "ZMM001": 14,
    "ZMB25": 24,
    "MB51-MEP": 22,
    "ZMM001-Extra": 14,
    "MaterialClasses": 9
}
    
functions = {
    "MB52": prepare_mb52,
    "MB51": prepare_mb51,
    "ZMM001": prepare_zmm001,
    "ZMB25": prepare_zmb25,
    "MB51-MEP": prepare_mb51mep,
    "ZMM001-Extra": prepare_zmm001,
    "MaterialClasses": prepare_materialclasses,
    "ZFI": prepare_zfi,
    "MCBA": prepare_mcba,
    "ZMRP": prepare_mrp
}