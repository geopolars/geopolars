============
GeoDataFrame
============
.. currentmodule:: geopolars

Constructor
-----------
.. autosummary::
   :toctree: api/

   GeoDataFrame

Attributes
----------

.. autosummary::
   :toctree: api/

   GeoDataFrame.geometry

..     DataFrame.columns
..     DataFrame.dtypes
..     DataFrame.height
..     DataFrame.schema
..     DataFrame.shape
..     DataFrame.width

Conversion
----------
.. autosummary::
   :toctree: api/

   GeoDataFrame.to_geopandas
   .. DataFrame.to_arrow
   .. DataFrame.to_dict
   .. DataFrame.to_dicts
   .. DataFrame.to_numpy
   .. DataFrame.to_pandas
   .. DataFrame.to_struct

Manipulation/ selection
-----------------------
.. autosummary::
   :toctree: api/

   GeoDataFrame.get_column

.. Methods of pandas ``Series`` objects are also available, although not
.. all are applicable to geometric objects and some may return a
.. ``Series`` rather than a ``GeoDataFrame`` result when appropriate. The methods
.. ``isna()`` and ``fillna()`` have been
.. implemented specifically for ``GeoDataFrame`` and are expected to work
.. correctly.
