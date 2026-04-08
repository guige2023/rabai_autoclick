# Copyright (c) 2024. coded by claude
"""Data Converter Action Module.

Converts data between different formats including XML to JSON,
CSV to dict, and custom format transformations.
"""
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import csv
import io
import logging

logger = logging.getLogger(__name__)


class ConversionFormat(Enum):
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    YAML = "yaml"
    DICT = "dict"


class DataConverter:
    @staticmethod
    def xml_to_dict(xml_string: str) -> Dict[str, Any]:
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(xml_string)
            return DataConverter._xml_to_dict(root)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML: {e}")

    @staticmethod
    def _xml_to_dict(element) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if element.attrib:
            result["@attributes"] = dict(element.attrib)
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text.strip()
            result["text"] = element.text.strip()
        for child in element:
            child_data = DataConverter._xml_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        return result

    @staticmethod
    def dict_to_xml(data: Dict[str, Any], root_tag: str = "root") -> str:
        import xml.etree.ElementTree as ET
        root = ET.Element(root_tag)
        DataConverter._dict_to_xml(root, data)
        return ET.tostring(root, encoding="unicode")

    @staticmethod
    def _dict_to_xml(parent, data: Dict[str, Any]) -> None:
        import xml.etree.ElementTree as ET
        for key, value in data.items():
            if key == "@attributes":
                parent.attrib.update(value)
                continue
            child = ET.SubElement(parent, key)
            if isinstance(value, dict):
                DataConverter._dict_to_xml(child, value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        DataConverter._dict_to_xml(child, item)
                    else:
                        child.text = str(item)
            else:
                child.text = str(value)

    @staticmethod
    def csv_to_dicts(csv_string: str, delimiter: str = ",") -> List[Dict[str, Any]]:
        reader = csv.DictReader(io.StringIO(csv_string), delimiter=delimiter)
        return list(reader)

    @staticmethod
    def dicts_to_csv(data: List[Dict[str, Any]], delimiter: str = ",") -> str:
        if not data:
            return ""
        output = io.StringIO()
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    @staticmethod
    def flatten_dict(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, dict):
                for sub_key, sub_value in DataConverter.flatten_dict(value, separator).items():
                    result[f"{key}{separator}{sub_key}"] = sub_value
            else:
                result[key] = value
        return result

    @staticmethod
    def convert_format(data: Any, from_format: ConversionFormat, to_format: ConversionFormat) -> Any:
        import json
        if from_format == to_format:
            return data
        if from_format == ConversionFormat.JSON and to_format == ConversionFormat.DICT:
            if isinstance(data, str):
                return json.loads(data)
            return data
        if from_format == ConversionFormat.DICT and to_format == ConversionFormat.JSON:
            return json.dumps(data)
        if from_format == ConversionFormat.JSON and to_format == ConversionFormat.XML:
            dict_data = json.loads(data) if isinstance(data, str) else data
            return DataConverter.dict_to_xml(dict_data)
        if from_format == ConversionFormat.XML and to_format == ConversionFormat.JSON:
            dict_data = DataConverter.xml_to_dict(data)
            return json.dumps(dict_data)
        raise ValueError(f"Conversion from {from_format} to {to_format} not supported")
