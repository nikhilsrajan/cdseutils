import os
import boto3
import functools
import multiprocessing as mp
import tqdm

from . import mydataclasses
from . import constants
from . import utils


VALID_S3URL_START = 's3://EODATA/'
VALID_S3URL_END = '.SAFE/'
EXT_JP2 = '.jp2'
EXT_SAFE = '.SAFE'
EXT_XML = '.xml'


def sentinel2_id_parser(sentinel2_id:str):
    sentinel2_id = sentinel2_id.removesuffix(EXT_SAFE)

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


def parse_s3url(s3url:str):
    s3path = utils.s3url_to_s3path(s3url=s3url)
    slash_splits = s3path.prefix.split('/')
    sentinel2_id = [x for x in slash_splits if EXT_SAFE in x][0] if EXT_SAFE in s3url else None
    band_filename = [x for x in slash_splits if EXT_JP2 in x][0] if EXT_JP2 in s3url else None
    xml_filename = [x for x in slash_splits if EXT_XML in x][0] if EXT_XML in s3url else None
    return {
        'id': sentinel2_id,
        'band_filename': band_filename,
        'xml_filename': xml_filename,
    }


def get_band_filename(
    sentinel2_id:str,
    band:str,
    ext:str = EXT_JP2,
    add_s2l2a_suffix:bool = False,
):
    # sentinel-2-l2a has this suffix present in its band names
    s2l2a_suffix = ''
    if add_s2l2a_suffix:
        if band in [
            constants.Bands.S2L2A.B01,
            constants.Bands.S2L2A.B05,
            constants.Bands.S2L2A.B06,
            constants.Bands.S2L2A.B07,
            constants.Bands.S2L2A.B8A,
            constants.Bands.S2L2A.B09,
            constants.Bands.S2L2A.B11,
            constants.Bands.S2L2A.B12,
            constants.Bands.S2L2A.SCL,
        ]:
            s2l2a_suffix = '_20m'
        elif band in [
            constants.Bands.S2L2A.B02,
            constants.Bands.S2L2A.B03,
            constants.Bands.S2L2A.B04,
            constants.Bands.S2L2A.B08,
        ]:
            s2l2a_suffix = '_10m'
        else:
            raise NotImplementedError(f'band = {band}')

    parsed_sentinel_id = sentinel2_id_parser(sentinel2_id = sentinel2_id)
    tile_number_field = parsed_sentinel_id['tile_number_field']
    datatake_sensing_startdate = parsed_sentinel_id['datatake_sensing_startdate']
    return f'{tile_number_field}_{datatake_sensing_startdate}_{band}{s2l2a_suffix}{ext}'


def parse_band_filename(
    sentinel2_band_filename:str,
    satellite:str = constants.Bands.S2L1C.NAME,
):
    filename, ext = sentinel2_band_filename.split('.')

    if satellite == constants.Bands.S2L1C.NAME:
        tile_number_field, \
        datatake_sensing_startdate, \
        band = filename.split('_')
    elif satellite == constants.Bands.S2L2A.NAME:
        tile_number_field, \
        datatake_sensing_startdate, \
        band, resolution = filename.split('_')
    else:
        raise NotImplementedError(f'satellite = {satellite}')

    return dict(
        tile_number_field = tile_number_field,
        datatake_sensing_startdate = datatake_sensing_startdate,
        band = band,
        ext = '.' + ext,
    )


def s3url_to_download_folderpath(
    s3url:str,
    root_folderpath:str,
):  
    if not s3url.startswith(VALID_S3URL_START):
        raise ValueError(f"Invalid s3url, valid s3url starts with '{VALID_S3URL_START}'")
    
    if not (s3url.endswith(VALID_S3URL_END) or s3url.endswith(EXT_SAFE)):
        raise ValueError(f"Invalid s3url, valid s3url ends with '{VALID_S3URL_END}' or '{EXT_SAFE}")

    ends_with = VALID_S3URL_END if s3url.endswith(VALID_S3URL_END) else EXT_SAFE
    
    download_folderpath = os.path.join(
        root_folderpath, *s3url.removeprefix(VALID_S3URL_START).removesuffix(ends_with).split('/')
    )

    return download_folderpath


def get_s3paths_single_url(
    s3url:str,
    s3_creds:mydataclasses.S3Credentials,
    root_folderpath:str,
    bands:list[str],
    satellite:str = constants.Bands.S2L1C.NAME,
) -> tuple[list[mydataclasses.S3Path], list[str]]:
    if not s3url.startswith(VALID_S3URL_START):
        raise ValueError(f"Invalid s3url, valid s3url starts with '{VALID_S3URL_START}'")
    
    if not (s3url.endswith(VALID_S3URL_END) or s3url.endswith(EXT_SAFE)):
        raise ValueError(f"Invalid s3url, valid s3url ends with '{VALID_S3URL_END}' or '{EXT_SAFE}")

    ends_with = VALID_S3URL_END if s3url.endswith(VALID_S3URL_END) else EXT_SAFE
    
    if satellite == constants.Bands.S2L1C.NAME:
        all_bands = constants.Bands.S2L1C.ALL
        add_s2l2a_suffix = False
    elif satellite == constants.Bands.S2L2A.NAME:
        all_bands = constants.Bands.S2L2A.ALL
        add_s2l2a_suffix = True
    else:
        raise NotImplementedError(f'satellite = {satellite}')
    
    invalid_bands = list(set(bands) - set(all_bands))

    if len(invalid_bands) > 0:
        raise ValueError(f"Invalid bands found: {invalid_bands}")
    
    sentinel2_id = parse_s3url(s3url=s3url)['id']

    filenames_to_download = [
        get_band_filename(
            sentinel2_id = sentinel2_id, 
            band = band, 
            ext = EXT_JP2,
            add_s2l2a_suffix = add_s2l2a_suffix,
        ) 
        for band in bands
    ]

    filenames_to_download.append('MTD_TL.xml') # metadata file for angles information

    s3 = boto3.resource(
        's3',
        endpoint_url = s3_creds.endpoint_url,
        aws_access_key_id = s3_creds.s3_access_key,
        aws_secret_access_key = s3_creds.s3_secret_key,
        region_name = s3_creds.region_name,
    )

    root_s3path = utils.s3url_to_s3path(s3url=s3url)

    all_files = s3.Bucket(root_s3path.bucket).objects.filter(Prefix=root_s3path.prefix)
    s3paths = [
        mydataclasses.S3Path(bucket=file.bucket_name, prefix=file.key) for file in all_files
        if any(file_to_download in file.key for file_to_download in set(filenames_to_download))
    ]

    download_folderpath = s3url_to_download_folderpath(
        s3url = s3url,
        root_folderpath = root_folderpath,
    )

    download_filepaths = []
    for s3path in s3paths:
        parsed_s3path = parse_s3url(s3url=utils.s3path_to_s3url(s3path=s3path))
        if EXT_JP2 in s3path.prefix:
            sentinel2_band_filename = parsed_s3path['band_filename']
            parsed_band_filename = parse_band_filename(
                sentinel2_band_filename = sentinel2_band_filename,
                satellite = satellite,
            )
            band = parsed_band_filename['band']
            ext = parsed_band_filename['ext']
            download_filepaths.append(os.path.join(download_folderpath, f"{band}{ext}"))
        elif EXT_XML in s3path.prefix:
            xml_filename = parsed_s3path['xml_filename']
            download_filepaths.append(os.path.join(download_folderpath, xml_filename))

    return s3paths, download_filepaths


def get_s3paths(
    s3urls:list[str],
    s3_creds:mydataclasses.S3Credentials,
    root_folderpath:str,
    bands:list[str],
    njobs:int = 16,
    satellite:str = constants.Bands.S2L1C.NAME,
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
    get_s3paths_single_url_partial = \
    functools.partial(
        get_s3paths_single_url,
        s3_creds = s3_creds,
        root_folderpath = root_folderpath,
        bands = bands,
        satellite = satellite,
    )

    unique_s3urls = list(set(s3urls))

    with mp.Pool(njobs) as p:
        list_of_tuple_of_lists = list(tqdm.tqdm(
            p.imap(get_s3paths_single_url_partial, unique_s3urls), 
            total=len(unique_s3urls)
        ))

    s3paths = []
    download_filepaths = []
    for _s3paths, _download_filepaths in list_of_tuple_of_lists:
        s3paths += _s3paths
        download_filepaths += _download_filepaths
    
    return s3paths, download_filepaths

