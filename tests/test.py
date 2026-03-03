import datetime
import sentinelhub
import geopandas as gpd

import cdseutils.utils


CDSE_JSON_FILEPATH = '/Users/nikhilsrajan/NASA-Harvest/project/fetch_satdata/data/cdse_credentials.json'
ROI_FILEPATH = '/Users/nikhilsrajan/NASA-Harvest/project/fetch_satdata/data/ukraine/EBRD_DATA/kernel_operations.shp'
START_DATE_STR = '2019-06-01'
END_DATE_STR = '2019-06-15'

START_DATE = datetime.datetime.strptime(START_DATE_STR, '%Y-%m-%d')
END_DATE = datetime.datetime.strptime(END_DATE_STR, '%Y-%m-%d')

cdse_creds = cdseutils.utils.cdse_credentials_from_json(CDSE_JSON_FILEPATH)

satellite = cdseutils.constants.Bands.S2L2A.NAME
bands = cdseutils.constants.Bands.S2L2A.ALL
# bands = ['B04', 'B08', 'SCL']
collection = sentinelhub.DataCollection.SENTINEL2_L2A

shapes_gdf = gpd.read_file(ROI_FILEPATH)

catalog_gdf = cdseutils.utils.query_catalog(
    shapes_gdf = shapes_gdf,
    sh_creds = cdse_creds.sh_creds,
    collection = collection,
    startdate = START_DATE,
    enddate = END_DATE,
)

print(catalog_gdf)

