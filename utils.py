import sentinelhub
import datetime
import geopandas as gpd
import pandas as pd
import os
import boto3
import multiprocessing as mp
import functools
import tqdm
import shapely.ops
import logging
import json

from . import constants
from . import mydataclasses


# Number of concurrent connections limit (IAD): 4
# see: https://documentation.dataspace.copernicus.eu/Quotas.html
MAX_CONCURRENT_CONNECTIONS = 4

EPSG_4326 = 'epsg:4326'


def create_config(
    sh_creds:mydataclasses.SHCredentials,
):
    config = sentinelhub.SHConfig()
    config.sh_client_id = sh_creds.sh_clientid
    config.sh_client_secret = sh_creds.sh_clientsecret
    config.sh_token_url = constants.SH_TOKEN_URL
    config.sh_base_url = constants.SH_BASE_URL
    return config


def fetch_catalog_single_bbox(
    bbox:sentinelhub.BBox,
    sh_creds:mydataclasses.SHCredentials,
    collection:sentinelhub.DataCollection,
    startdate:datetime.datetime,
    enddate:datetime.datetime,
    filter:str = None,
    fields:dict = None,
    cache_folderpath:str = None,
):
    # Doing this manually here so that the cache foldername is consistent.
    # This is anyway being done by catalog.search (see docs for sentinelhub.SentinelHubCatalog.search)
    if bbox and bbox.crs is not sentinelhub.CRS.WGS84:
        bbox = bbox.transform_bounds(sentinelhub.CRS.WGS84)

    save_catalog_filepath = None
    save_results_filepath = None
    if cache_folderpath is not None:
        _foldername = '+'.join(
            [collection.catalog_id]
            + [str(x) for x in list(bbox)]
            + [startdate.strftime('%Y%m%dT%H%M%S'), 
               enddate.strftime('%Y%m%dT%H%M%S')]
        )
        current_query_cache_folderpath = os.path.join(
            cache_folderpath, _foldername
        )
        os.makedirs(current_query_cache_folderpath, exist_ok=True)
        if not os.access(current_query_cache_folderpath, os.W_OK):
            raise PermissionError(
                f'Change current_query_cache_folderpath as user '
                f'does not have permission to write to {current_query_cache_folderpath}.'
            )
        save_catalog_filepath = os.path.join(
            current_query_cache_folderpath,
            'catalog.geojson'
        )
        save_results_filepath = os.path.join(
            current_query_cache_folderpath,
            'results.json'
        )

    if save_catalog_filepath is not None and save_results_filepath is not None:
        if os.path.exists(save_catalog_filepath) and os.path.exists(save_results_filepath):
            catalog_gdf = gpd.read_file(save_catalog_filepath)
            with open(save_results_filepath) as h:
                results = json.load(h)
            return catalog_gdf, results

    config = create_config(sh_creds=sh_creds)

    catalog = sentinelhub.SentinelHubCatalog(config=config)

    search_iterator = catalog.search(
        collection=collection,
        bbox=bbox,
        time=(startdate, enddate),
        filter=filter,
        fields=fields,
    )

    results = list(search_iterator)

    catalog_gdf = gpd.GeoDataFrame(data={
        'id': search_iterator.get_ids(),
        'timestamp': search_iterator.get_timestamps(),
        'geometry': [x.geometry for x in search_iterator.get_geometries()],
        's3url': [res['assets']['data']['href'] for res in results],
    }, crs='epsg:4326')

    if cache_folderpath is not None:
        catalog_gdf.to_file(save_catalog_filepath)
        with open(save_results_filepath, 'w') as h:
            json.dump(results, h)

    return catalog_gdf, results


def _get_unique_bboxes(
    bboxes:list[sentinelhub.BBox]
):
    return [
        sentinelhub.BBox(bbox_tuple, crs=sentinelhub.CRS.WGS84) 
        for bbox_tuple in set(
            tuple(bbox.transform_bounds(sentinelhub.CRS.WGS84)) 
            for bbox in bboxes
        )
    ]


def fetch_catalog(
    bboxes:list[sentinelhub.BBox],
    sh_creds:mydataclasses.SHCredentials,
    collection:sentinelhub.DataCollection,
    startdate:datetime.datetime,
    enddate:datetime.datetime,
    filter:str=None,
    fields:dict=None,
    cache_folderpath:str = None,
):
    unique_bboxes = _get_unique_bboxes(bboxes=bboxes)

    catalog_gdfs = []
    results = []
    
    for bbox in unique_bboxes:
        _catalog_gdf, _results = fetch_catalog_single_bbox(
            bbox = bbox,
            sh_creds = sh_creds,
            collection = collection,
            startdate = startdate,
            enddate = enddate,
            filter = filter,
            fields = fields,
            cache_folderpath = cache_folderpath,
        )
        catalog_gdfs.append(_catalog_gdf)
        results += _results

    catalog_gdf = gpd.GeoDataFrame(
        pd.concat(catalog_gdfs).reset_index(drop=True),
        crs = catalog_gdfs[0].crs
    )

    return catalog_gdf, results
    

def download_data(
    collection:str,
    sh_creds:mydataclasses.SHCredentials,
    startdate:datetime.datetime,
    enddate:datetime.datetime,
    data_folder:str,
    resolution:int,
    bbox:sentinelhub.geometry.BBox,
    mosaicking_order=sentinelhub.MosaickingOrder.LEAST_CC, # how does this change across collections?
    raise_download_errors:bool=True,
    show_progress:bool=False,
):
    config = create_config(sh_creds=sh_creds)

    width, height = sentinelhub.bbox_to_dimensions(bbox, resolution=resolution)

    if width > 2500:
        raise ValueError(f"Invalid width: {width}. Must be less than or equal to 2500.")
    if height > 2500:
        raise ValueError(f"Invalid height: {height}. Must be less than or equal to 2500.")

    request = sentinelhub.SentinelHubRequest(
        data_folder=data_folder,
        evalscript=constants.EVALSCRIPTS[collection],
        input_data=[
            sentinelhub.SentinelHubRequest.input_data(
                data_collection=constants.DATACOLLECTIONS[collection],
                time_interval=(startdate, enddate),
                mosaicking_order=mosaicking_order,
            )
        ],
        responses=[sentinelhub.SentinelHubRequest.output_response("default", sentinelhub.MimeType.TIFF)],
        bbox=bbox,
        size=(width, height),
        config=config,
    )

    request.save_data(
        raise_download_errors=raise_download_errors,
        show_progress=show_progress,
    )

    request_id = request.get_filename_list()[0].split('/')[0]

    tiff_filepath = os.path.join(data_folder, request_id, 'response.tiff')
    json_filepath = os.path.join(data_folder, request_id, 'request.json')

    if not os.path.exists(tiff_filepath):
        raise ValueError(
            'Something went wrong. Tiff file not found in expected path.\n'
            f'Expected filepath: {tiff_filepath}'
        )
    if not os.path.exists(json_filepath):
        raise ValueError(
            'Something went wrong. Json file not found in expected path.\n'
            f'Expected filepath: {json_filepath}'
        )

    return tiff_filepath, json_filepath


def download_data_s2l1c(
    sh_creds:mydataclasses.SHCredentials,
    startdate:datetime.datetime,
    enddate:datetime.datetime,
    data_folder:str,
    bbox:sentinelhub.geometry.BBox,
    resolution:int=10,
    mosaicking_order=sentinelhub.MosaickingOrder.LEAST_CC,
    raise_download_errors:bool=True,
    show_progress:bool=False,
):
    return download_data(
        collection=constants.S2L1C,
        sh_creds=sh_creds,
        startdate=startdate,
        enddate=enddate,
        data_folder=data_folder,
        bbox=bbox,
        resolution=resolution,
        mosaicking_order=mosaicking_order,
        raise_download_errors=raise_download_errors,
        show_progress=show_progress,
    )


def download_s3_file(
    s3_creds:mydataclasses.S3Credentials,
    s3_path:mydataclasses.S3Path,
    download_filepath:str = None,
    download_folderpath:str = None,
    overwrite:bool = False,
    print_messages:bool = True,
):
    def _print(*args, end='\n', sep=' '):
        if print_messages:
            print(*args, end=end, sep=sep)

    if download_folderpath is None and download_filepath is None:
        raise Exception(
            "Either 'download_folderpath' or 'download_filepath' " + \
            "should be non None."
        )

    if download_filepath is None:
        filename = s3_path.prefix.split('/')[-1]
        download_filepath = os.path.join(
            download_folderpath, filename,
        )
    else:
        download_folderpath = os.path.split(download_filepath)[0]
    
    os.makedirs(download_folderpath, exist_ok=True)

    file_exists = os.path.exists(download_filepath)

    _print('---------------------')
    _print('File set to download:')
    _print('---------------------')
    _print('bucket:', s3_path.bucket)
    _print('prefix:', s3_path.prefix)
    _print('---------------------')
    
    if not file_exists or overwrite:
        if file_exists:
            _print('Re-', end='', sep='')
        _print('Downloading file...')

        s3 = boto3.resource(
            's3',
            endpoint_url = s3_creds.endpoint_url,
            aws_access_key_id = s3_creds.s3_access_key,
            aws_secret_access_key = s3_creds.s3_secret_key,
            region_name = s3_creds.region_name
        )
        bucket = s3.Bucket(s3_path.bucket)
        bucket.download_file(s3_path.prefix, download_filepath)
    else:
        _print('File already downloaded.')
    
    _print('---------------------')

    return download_filepath


def _download_s3_file_by_tuple(
    s3_path_download_filepath_tuple:tuple[mydataclasses.S3Path, str],
    s3_creds:mydataclasses.S3Credentials,
    overwrite:bool = False,
    logger:logging.Logger = None,
):
    s3_path, download_filepath = s3_path_download_filepath_tuple

    try:
        download_s3_file(
            s3_creds = s3_creds,
            s3_path = s3_path,
            download_filepath = download_filepath,
            overwrite = overwrite,
            print_messages = False,
        )
        download_success = os.path.exists(download_filepath)

        if logger is not None:
            logger.info(f"download_s3_file -- {s3_path.bucket} -- {s3_path.prefix} -- {download_filepath} -- success")

    except Exception as e:
        print(f'Encountered error: {e}')
        download_success = False
        
        if logger is not None:
            logger.info(f"download_s3_file -- {s3_path.bucket} -- {s3_path.prefix} -- {download_filepath} -- failed")
            logger.error(f"Error encountered: \"{e}\" -- download_s3_file(s3_path={s3_path}, download_filepath={download_filepath})")

    return download_success
    

def download_s3_files(
    s3_creds:mydataclasses.S3Credentials,
    s3_paths:list[mydataclasses.S3Path],
    download_filepaths:list[str],
    overwrite:bool = False,
    logger:logging.Logger = None,
):
    if len(s3_paths) != len(download_filepaths):
        raise ValueError('Size of s3_paths and download_filepaths do not match.')
    
    download_s3_file_by_tuple_partial = functools.partial(
        _download_s3_file_by_tuple,
        s3_creds = s3_creds,
        overwrite = overwrite,
    )

    s3_path_download_filepath_tuples = list(zip(s3_paths, download_filepaths))

    with mp.Pool(MAX_CONCURRENT_CONNECTIONS) as p:
        download_successes = list(tqdm.tqdm(
            p.imap(download_s3_file_by_tuple_partial, s3_path_download_filepath_tuples), 
            total=len(s3_path_download_filepath_tuples)
        ))
    
    print(f"Successful downloads: {sum(download_successes)} / {len(download_successes)}")
    
    return download_successes


def sentinel2_id_parser(sentinel2_id:str):
    sentinel2_id = sentinel2_id.replace('.SAFE', '')

    # see: https://sentinels.copernicus.eu/en/web/sentinel/user-guides/sentinel-2-msi/naming-convention
    # Compact Naming Convention
    mission_identifier, \
    product_level, \
    datatake_sensing_startdate, \
    processing_baseline_number, \
    relative_orbit_number, \
    tile_number_field, \
    product_discriminator = sentinel2_id.split('_')

    return dict(
        mission_identifier = mission_identifier,
        product_level = product_level,
        datatake_sensing_startdate = datatake_sensing_startdate,
        processing_baseline_number = processing_baseline_number,
        relative_orbit_number = relative_orbit_number,
        tile_number_field = tile_number_field,
        product_discriminator = product_discriminator,
    )


def get_sentinel2_band_filename(
    sentinel2_id:str,
    band:str,
    ext:str='.jp2',
):
    parsed_sentinel_id = sentinel2_id_parser(sentinel2_id = sentinel2_id)
    tile_number_field = parsed_sentinel_id['tile_number_field']
    datatake_sensing_startdate = parsed_sentinel_id['datatake_sensing_startdate']
    return f'{tile_number_field}_{datatake_sensing_startdate}_{band}{ext}'


def parse_sentinel2_band_filename(
    sentinel2_band_filename:str,
):
    filename, ext = sentinel2_band_filename.split('.')
    tile_number_field, \
    datatake_sensing_startdate, \
    band = filename.split('_')

    return dict(
        tile_number_field = tile_number_field,
        datatake_sensing_startdate = datatake_sensing_startdate,
        band = band,
    )


def get_sentinel2_s3_paths_single_url(
    s3_url:str,
    s3_creds:mydataclasses.S3Credentials,
    root_folderpath:str,
    bands:list[str],
) -> tuple[list[mydataclasses.S3Path], list[str]]:
    VALID_START = 's3://EODATA/'
    VALID_END = '.SAFE/'
    EXT = '.jp2'

    if not s3_url.startswith(VALID_START):
        raise ValueError(f"Invalid s3_url, valid s3_url starts with '{VALID_START}'")
    
    if not s3_url.endswith(VALID_END):
        raise ValueError(f"Invalid s3_url, valid s3_url ends with '{VALID_END}'")
    
    invalid_bands = list(set(bands) - set(constants.Bands.Sentinel2.ALL))

    if len(invalid_bands) > 0:
        raise ValueError(f"Invalid bands found: {invalid_bands}")
    
    sentinel2_id = s3_url.replace(VALID_END, '').split('/')[-1]

    filenames_to_download = [
        get_sentinel2_band_filename(
            sentinel2_id = sentinel2_id, 
            band = band, 
            ext = EXT,
        ) 
        for band in bands
    ]

    s3 = boto3.resource(
        's3',
        endpoint_url = s3_creds.endpoint_url,
        aws_access_key_id = s3_creds.s3_access_key,
        aws_secret_access_key = s3_creds.s3_secret_key,
        region_name = s3_creds.region_name,
    )

    all_files = s3.Bucket("eodata").objects.filter(Prefix=s3_url.replace(VALID_START, ''))
    s3_paths = [
        mydataclasses.S3Path(bucket=file.bucket_name, prefix=file.key) for file in all_files
        if any(file_to_download in file.key for file_to_download in set(filenames_to_download))
    ]

    download_folderpath = os.path.join(
        root_folderpath, *s3_url.replace(VALID_START, '').replace(VALID_END, '').split('/')
    )

    download_filepaths = []
    for s3_path in s3_paths:
        band = parse_sentinel2_band_filename(
            sentinel2_band_filename = s3_path.prefix.split('/')[-1]
        )['band']
        download_filepaths.append(os.path.join(download_folderpath, f"{band}{EXT}"))

    return s3_paths, download_filepaths


def get_sentinel2_s3_paths(
    s3_urls:list[str],
    s3_creds:mydataclasses.S3Credentials,
    root_folderpath:str,
    bands:list[str],
    njobs:int = 16,
) -> tuple[list[mydataclasses.S3Path], list[str]]:
    """
    Ran a tiny experiment to get the following times:
    -  4 jobs -> 60s
    -  8 jobs -> 30s
    - 16 jobs -> 18s
    - 32 jobs -> 23s
    - 64 jobs -> 28s

    Thus set the default njobs to be 16.
    """
    get_sentinel2_s3_paths_single_url_partial = \
    functools.partial(
        get_sentinel2_s3_paths_single_url,
        s3_creds = s3_creds,
        root_folderpath = root_folderpath,
        bands = bands,
    )

    unique_s3_urls = list(set(s3_urls))

    with mp.Pool(njobs) as p:
        list_of_tuple_of_lists = list(tqdm.tqdm(
            p.imap(get_sentinel2_s3_paths_single_url_partial, unique_s3_urls), 
            total=len(unique_s3_urls)
        ))

    s3_paths = []
    download_filepaths = []
    for _s3_paths, _download_filepaths in list_of_tuple_of_lists:
        s3_paths += _s3_paths
        download_filepaths += _download_filepaths
    
    return s3_paths, download_filepaths


def reduce_geometries(
    shapes_gdf:gpd.GeoDataFrame,
):
    union_geom = shapely.ops.unary_union(shapes_gdf['geometry'])
    if isinstance(union_geom, shapely.MultiPolygon):
        geometries = list(union_geom.geoms)
    elif isinstance(union_geom, shapely.Polygon):
        geometries = [union_geom]
    else:
        raise NotImplementedError(f'Unhandelled geometry type encountered: {union_geom.geom_type}')
    return gpd.GeoDataFrame(
        data = {'geometry': geometries},
        crs = shapes_gdf.crs,
    )


def get_bboxes(
    shapes_gdf:gpd.GeoDataFrame,
):  
    # converting to EPSG_4326 since catalog search converts the bbox to 
    # that crs before conducting search
    reduced_shapes_gdf = reduce_geometries(shapes_gdf).to_crs(EPSG_4326)
    bboxes = [
        sentinelhub.BBox(geom.bounds, crs=reduced_shapes_gdf.crs)
        for geom in reduced_shapes_gdf['geometry']
    ]
    return bboxes
