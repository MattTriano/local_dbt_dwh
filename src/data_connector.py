from dataclasses import dataclass
import datetime as dt
import json
from typing import Dict, Optional, Union

import requests


@dataclass
class SocrataTable:
    table_id: str
    table_name: str
    download_format: Optional[str] = None

class SocrataTableMetadata:
    def __init__(
        self,
        socrata_table: SocrataTable,
    ):
        self.socrata_table = socrata_table
        self.table_id = self.socrata_table.table_id
        self.metadata = self.get_table_metadata()

    def get_table_metadata(self) -> Dict:
        api_call = f"http://api.us.socrata.com/api/catalog/v1?ids={self.table_id}"
        response = requests.get(api_call)
        if response.status_code == 200:
            response_json = response.json()
            metadata = {
                "_id": self.table_id,
                "time_of_collection": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            metadata.update(response_json["results"][0])
            return metadata
        else:
            raise Exception(
                f"Request for metadata for table {self.table_id} failed with status code {response.status_code}"
            )

    def get_table_metadata_attr(self, attr_dict: dict, attr_name: str) -> str:
        try:
            if attr_name in attr_dict.keys():
                return attr_dict[attr_name]
            else:
                return None
        except Exception as err:
            print(f"Exception {err} with type {type(err)} raised for attr_name '{attr_name}'.")
            print("Did you mean to enter attr_dict['resource']")
            raise

    @property
    def resource_metadata(self):
        return self.get_table_metadata_attr(attr_dict=self.metadata, attr_name="resource")

    @property
    def table_name(self) -> str:
        if self.socrata_table.table_name is None:
            table_name = self.get_table_metadata_attr(
                a_dict=self.resource_metadata, attr_name="name"
            )
            return "_".join(re.sub("[^0-9a-zA-Z]+", "", table_name.lower()).split())
        else:
            return self.socrata_table.table_name

    @property
    def data_domain(self) -> str:
        metadatas_metadata = self.get_table_metadata_attr(
            attr_dict=self.metadata, attr_name="metadata"
        )
        return self.get_table_metadata_attr(attr_dict=metadatas_metadata, attr_name="domain")

    @property
    def column_details(self) -> Dict:
        if self.resource_metadata is not None:
            column_metadata_fields = [
                "columns_name",
                "columns_field_name",
                "columns_datatype",
                "columns_description",
                "columns_format",
            ]
            present_col_metadata_fields = [
                el for el in column_metadata_fields if el in self.resource_metadata.keys()
            ]
            if len(present_col_metadata_fields) > 0:
                return {
                    field: self.resource_metadata[field] for field in present_col_metadata_fields
                }
        return {}

    @property
    def table_has_geospatial_feature(self) -> bool:
        socrata_geo_datatypes = [
            "Line",
            "Location",
            "MultiLine",
            "MultiPoint",
            "MultiPolygon",
            "Point",
            "Polygon",
        ]
        if self.column_details is not None:
            if "columns_datatype" in self.column_details.keys():
                column_datatypes = self.get_table_metadata_attr(
                    attr_dict=self.column_details, attr_name="columns_datatype"
                )
                return any([col_dtype in socrata_geo_datatypes for col_dtype in column_datatypes])
        return False

    @property
    def table_has_geo_type_view(self) -> bool:
        table_view_type = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="lens_view_type"
        )
        return table_view_type == "geo"

    @property
    def table_has_map_type_display(self) -> bool:
        table_display_type = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="lens_display_type"
        )
        return table_display_type == "map"

    @property
    def table_has_data_columns(self) -> bool:
        table_data_cols = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="columns_name"
        )
        return len(table_data_cols) != 0

    @property
    def is_geospatial(self) -> bool:
        return (
            (not self.table_has_data_columns)
            and (self.table_has_geo_type_view or self.table_has_map_type_display)
        ) or (self.table_has_geospatial_feature)

    def get_valid_download_formats(self) -> Dict:
        valid_download_formats = {
            "flat": {
                "CSV": "csv",
                "TSV": "tsv",
            },
            "gis": {
                "shp": "Shapefile",
                "shapefile": "Shapefile",
                "geojson": "GeoJSON",
                "kmz": "KMZ",
                "kml": "KML",
            },
        }
        return valid_download_formats

    @property
    def download_format(self) -> str:
        if self.socrata_table.download_format is None:
            if self.is_geospatial:
                return "GeoJSON"
            else:
                return "csv"
        else:
            download_format = self.socrata_table.download_format.lower()
            self.assert_download_format_is_supported(download_format=download_format)
            valid_download_formats = self.get_valid_download_formats()
            if download_format in valid_download_formats.values():
                return download_format
            elif download_format in valid_download_formats.keys():
                return valid_download_formats[download_format]
            else:
                raise Exception("Very invalid download format (should have already been caught)")

    def standardize_datetime_str_repr(self, datetime_obj: Union[str, dt.datetime]) -> str:
        if isinstance(datetime_obj, str):
            datetime_obj = dt.datetime.strptime(datetime_obj, "%Y-%m-%dT%H:%M:%S.%fZ")
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def latest_data_update_datetime(self) -> str:
        data_updated_at = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="data_updated_at"
        )
        if data_updated_at is not None:
            return self.standardize_datetime_str_repr(datetime_obj=data_updated_at)
        return None

    @property
    def latest_metadata_update_datetime(self) -> str:
        metadata_updated_at = self.get_table_metadata_attr(
            attr_dict=self.resource_metadata, attr_name="metadata_updated_at"
        )
        if metadata_updated_at is not None:
            return self.standardize_datetime_str_repr(datetime_obj=metadata_updated_at)
        return None

    @property
    def data_download_url(self) -> str:
        if self.is_geospatial:
            return f"https://{self.data_domain}/api/geospatial/{self.table_id}?method=export&format={self.download_format}"
        else:
            return f"https://{self.data_domain}/api/views/{self.table_id}/rows.{self.download_format}?accessType=DOWNLOAD"