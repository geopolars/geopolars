GeoPolars |version|
===================

GeoPolars extends the `Polars <https://github.com/pola-rs/polars>`_ DataFrame library for use with geospatial data.

Description
-----------

With heavy inspiration from `GeoPandas`_, GeoPolars has two main goals:

- Faster multithreaded operations than GeoPandas while keeping an easy-to-use, high-level interface.
- Better data interoperability without copies, due to its `GeoArrow`_ core.

At this point, GeoPolars is a **prototype** and should not be considered production-ready.


.. _GeoPandas: https://geopandas.org/
.. _GeoArrow: https://github.com/geoarrow/geoarrow


Installation
------------

GeoPolars can be installed from PyPI:

::

  pip install --pre geopolars

(``-pre`` is necessary to allow installation of an alpha release). The publishing processs includes
binary wheels for many platforms, so it should be easy to install, without needing to compile the
underlying Rust code from source.


API Reference
-------------

.. toctree::
   :maxdepth: 1

   reference/index

Useful links
------------

`Binary Installers (PyPI) <https://pypi.org/project/geopolars/>`_ | `Source Repository (GitHub) <https://github.com/geopolars/geopolars>`_ | `Issues & Ideas <https://github.com/geopolars/geopolars/issues>`_
