import json
from dataclasses import dataclass

from arro3.core import ChunkedArray, Field
from polars import Series

from geopolars.internals.enums import GeoArrowExtensionName


@dataclass
class SeriesWrapper:
    s: Series
    geoarrow_type: GeoArrowExtensionName
    geoarrow_metadata: dict[str, str]

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        Export a Series via the Arrow PyCapsule Interface.

        https://arrow.apache.org/docs/dev/format/CDataInterface/PyCapsuleInterface.html
        """
        ca = ChunkedArray.from_arrow(self.s.to_arrow())
        metadata: dict[str, str] = {
            "ARROW:extension:name": self.geoarrow_type,
        }
        if self.geoarrow_metadata:
            metadata["ARROW:extension:metadata"] = json.dumps(self.geoarrow_metadata)
        field = Field("", type=ca.type, nullable=True, metadata=metadata)

        ca_with_ext_meta = ChunkedArray(ca.chunks, field)
        return ca_with_ext_meta.__arrow_c_stream__(requested_schema)
