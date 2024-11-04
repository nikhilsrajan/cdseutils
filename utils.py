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
WGS_84 = 'wgs84' # same as epsg:4326 but sentinelhub raises warning for using epsg:4326


def cdse_credentials_to_dict(
    cdse_creds:mydataclasses.Credentials,
):
    return dict(
        sh_clientid = cdse_creds.sh_creds.sh_clientid,
        sh_clientsecret = cdse_creds.sh_creds.sh_clientsecret,
        s3_access_key = cdse_creds.s3_creds.s3_access_key,
        s3_secret_key = cdse_creds.s3_creds.s3_secret_key,
    )


def cdse_credentials_from_dict(cdse_creds_dict:dict):
    return mydataclasses.Credentials(
        cdse_clientid = cdse_creds_dict['sh_clientid'],
        cdse_clientsecret = cdse_creds_dict['sh_clientsecret'],
        cdse_s3_access_key = cdse_creds_dict['s3_access_key'],
        cdse_s3_secret_key = cdse_creds_dict['s3_secret_key'],
    )


def cdse_credentials_to_json(
    cdse_creds:mydataclasses.Credentials,
    filepath:str,
):
    with open(filepath, 'w') as h:
        json.dump(cdse_credentials_to_dict(
            cdse_creds = cdse_creds,
        ), h)


def cdse_credentials_from_json(
    filepath:str,
):
    with open(filepath, 'r') as h:
        cdse_creds = cdse_credentials_from_dict(json.load(h))
    return cdse_creds


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
    if bbox.crs is not sentinelhub.CRS.WGS84:
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


S3_DOWNLOAD_FAILED = 'return::status-download-s3-failed'
S3_DOWNLOAD_SUCCESS = 'return::status-download-s3-success'
S3_DOWNLOAD_SKIPPED = 'return::status-download-s3-skipped'
S3_DOWNLOAD_OVERWRITE = 'return::status-download-s3-overwrite'


def download_s3_file(
    s3_creds:mydataclasses.S3Credentials,
    s3path:mydataclasses.S3Path,
    download_filepath:str = None,
    download_folderpath:str = None,
    overwrite:bool = False,
    print_messages:bool = True,
    raise_error:bool = True,
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
        filename = s3path.prefix.split('/')[-1]
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
    _print('bucket:', s3path.bucket)
    _print('prefix:', s3path.prefix)
    _print('---------------------')
    
    if not file_exists or overwrite:
        if file_exists:
            _print('Re-', end='', sep='')
        _print('Downloading file...')
        try:
            s3 = boto3.resource(
                's3',
                endpoint_url = s3_creds.endpoint_url,
                aws_access_key_id = s3_creds.s3_access_key,
                aws_secret_access_key = s3_creds.s3_secret_key,
                region_name = s3_creds.region_name
            )
            bucket = s3.Bucket(s3path.bucket)
            bucket.download_file(s3path.prefix, download_filepath)
            ret = S3_DOWNLOAD_OVERWRITE if file_exists else S3_DOWNLOAD_SUCCESS
        except Exception as e:
            if raise_error:
                raise e
            else:
                ret = S3_DOWNLOAD_FAILED
    else:
        _print('File already downloaded.')
        ret = S3_DOWNLOAD_SKIPPED
    
    _print('---------------------')

    return download_filepath, ret


def _download_s3_file_by_tuple(
    s3path_download_filepath_tuple:tuple[mydataclasses.S3Path, str],
    s3_creds:mydataclasses.S3Credentials,
    overwrite:bool = False,
    logger:logging.Logger = None,
):
    s3path, download_filepath = s3path_download_filepath_tuple

    try:
        _, ret = download_s3_file(
            s3_creds = s3_creds,
            s3path = s3path,
            download_filepath = download_filepath,
            overwrite = overwrite,
            print_messages = False,
        )

        if logger is not None:
            logger.info(f"download_s3_file -- {s3path.bucket} -- {s3path.prefix} -- {download_filepath} -- success")

    except Exception as e:
        print(f'Encountered error: {e}')
        ret = S3_DOWNLOAD_FAILED
        
        if logger is not None:
            logger.info(f"download_s3_file -- {s3path.bucket} -- {s3path.prefix} -- {download_filepath} -- failed")
            logger.error(f"Error encountered: \"{e}\" -- download_s3_file(s3path={s3path}, download_filepath={download_filepath})")

    return ret
    

def download_s3_files(
    s3_creds:mydataclasses.S3Credentials,
    s3paths:list[mydataclasses.S3Path],
    download_filepaths:list[str],
    overwrite:bool = False,
    logger:logging.Logger = None,
):
    if len(s3paths) != len(download_filepaths):
        raise ValueError('Size of s3paths and download_filepaths do not match.')
    
    download_s3_file_by_tuple_partial = functools.partial(
        _download_s3_file_by_tuple,
        s3_creds = s3_creds,
        overwrite = overwrite,
        logger = logger,
    )

    s3path_download_filepath_tuples = list(zip(s3paths, download_filepaths))

    with mp.Pool(MAX_CONCURRENT_CONNECTIONS) as p:
        download_statuses = list(tqdm.tqdm(
            p.imap(download_s3_file_by_tuple_partial, s3path_download_filepath_tuples), 
            total=len(s3path_download_filepath_tuples)
        ))
    

    
    # print(f"Successful downloads: {sum(download_successes)} / {len(download_successes)}")
    
    return download_statuses


def reduce_geometries(
    shapes_gdf:gpd.GeoDataFrame,
):
    union_geom_convexhull = shapely.ops.unary_union(shapes_gdf['geometry']).convex_hull
    return gpd.GeoDataFrame(
        data = {'geometry': [union_geom_convexhull]},
        crs = shapes_gdf.crs,
    )


def get_bboxes(
    shapes_gdf:gpd.GeoDataFrame,
):  
    # converting to WGS_84 since catalog search converts the bbox to 
    # that crs before conducting search
    reduced_shapes_gdf = reduce_geometries(shapes_gdf).to_crs(WGS_84)
    bboxes = [
        sentinelhub.BBox(geom.bounds, crs=reduced_shapes_gdf.crs)
        for geom in reduced_shapes_gdf['geometry']
    ]

    bboxes = _get_unique_bboxes(bboxes=bboxes)

    return bboxes


def s3path_to_s3url(s3path:mydataclasses.S3Path, ):
    return f's3://{s3path.bucket}/{s3path.prefix}'


def s3url_to_s3path(s3url:str):
    if not s3url.startswith('s3://'):
        raise ValueError("s3url must always start with 's3://'")
    s3url = s3url.removeprefix('s3://')
    slash_splits = s3url.split('/')
    bucket = slash_splits[0]
    prefix = '/'.join(slash_splits[1:])
    return mydataclasses.S3Path(bucket=bucket, prefix=prefix)
