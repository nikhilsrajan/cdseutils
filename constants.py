import sentinelhub

from . import evalscripts


# URLs
SH_BASE_URL = "https://sh.dataspace.copernicus.eu"
SH_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

# s3 constants
## see: https://documentation.dataspace.copernicus.eu/APIs/S3.html
S3_ENDPOINT_URL = "https://eodata.dataspace.copernicus.eu"
S3_REGIONNAME = 'default'

# Collections
S2L1C = 's2l1c' # Sentinel-2 L1C

# Data Collections
DATACOLLECTIONS = {
    S2L1C: sentinelhub.DataCollection.SENTINEL2_L1C.define_from(S2L1C, service_url=SH_BASE_URL)
}

# Evalscripts
EVALSCRIPTS = {
    S2L1C: evalscripts.S2L1C_ALL_BANDS,
}

# Bands
class Bands:
    class Sentinel2:
        NAME = 'sentinel2'
        B01 = 'B01'
        B02 = 'B02'
        B03 = 'B03'
        B04 = 'B04'
        B05 = 'B05'
        B06 = 'B06'
        B07 = 'B07'
        B08 = 'B08'
        B8A = 'B8A'
        B09 = 'B09'
        B10 = 'B10'
        B11 = 'B11'
        B12 = 'B12'
        ALL = [
            B01, B02, B03, B04, B05, B06, B07,
            B08, B8A, B09, B10, B11, B12,
        ]
        
