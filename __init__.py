"""
Utility functions for "Copernicus Data Space Ecosystem" (CDSE) using SentinelHub package.

SentinelHub package has two backend providers -- planet labs and sinergise. CDSE uses the
one from sinergise.

The documentation of the package and APIs from the two providers are as follows:
- sinergise: https://sentinelhub-py.readthedocs.io/en/latest/index.html
- planet labs: https://docs.sentinel-hub.com/api/latest/

Although planet labs provides more data sources (eg: "Harmonized Landsat and Sentinel-2")
and more functionalities (eg: s2cloudless output for sentinel-2 images) the appeal for 
using CDSE comes from the monthly credits that CDSE provides. Planet labs' free tier has
monthly 5000 credits (see: https://www.sentinel-hub.com/pricing/#tab-plans) where as the
free account from CDSE which is the "Copernicus General User" has monthly 30000 credits
(see: https://documentation.dataspace.copernicus.eu/Quotas.html).
"""


from . import utils
from . import constants
