from enum import Enum


class GeoArrowExtensionName(str, Enum):
    """GeoArrow extension name"""

    POINT = "geoarrow.point"
    LINESTRING = "geoarrow.linestring"
    POLYGON = "geoarrow.polygon"
    MULTIPOINT = "geoarrow.multipoint"
    MULTILINESTRING = "geoarrow.multilinestring"
    MULTIPOLYGON = "geoarrow.multipolygon"
    BOX = "geoarrow.box"
    WKB = "geoarrow.wkb"
    WKT = "geoarrow.wkt"
    OGC_WKB = "ogc.wkb"

    def __str__(self):
        return self.value


class CoordinateDimension(str, Enum):
    XY = "xy"
    XYZ = "xyz"
    XYM = "xym"
    XYZM = "xyzm"
