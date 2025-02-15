import dataclasses

from . import constants

@dataclasses.dataclass
class S3Credentials:
    s3_access_key:str = None
    s3_secret_key:str = None
    endpoint_url:str = constants.S3_ENDPOINT_URL
    region_name:str = constants.S3_REGIONNAME
   

@dataclasses.dataclass
class SHCredentials:
    sh_clientid:str = None
    sh_clientsecret:str = None
   

class Credentials(object):
    """
    Credentials for Sentinel Hub services (client_id & client_secret) can be obtained
    in your Dashboard (see: https://shapps.dataspace.copernicus.eu/dashboard/#/). 
    In the User Settings you can create a new OAuth Client to generate these credentials. 
    For more detailed instructions, visit the relevant documentation page
    (see: https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Overview/Authentication.html).
    """
    def __init__(
        self,
        cdse_username:str = None,
        cdse_password:str = None,
        cdse_clientid:str = None,
        cdse_clientsecret:str = None,
        cdse_s3_access_key:str = None,
        cdse_s3_secret_key:str = None,
    ):
        ## CDSE, https://dataspace.copernicus.eu/
        ## This is commented out as they are not used in code.
        # self.cdse_username = cdse_username
        # self.cdse_password = cdse_password

        # sentinelhub, https://shapps.dataspace.copernicus.eu/dashboard/#/
        self.sh_creds = SHCredentials(
            sh_clientid = cdse_clientid,
            sh_clientsecret = cdse_clientsecret
        )

        # s3, https://eodata-s3keysmanager.dataspace.copernicus.eu/panel/s3-credentials
        ## Transfer limit: 12 (TB/month); Transfer bandwidth: 20 (Mbps)
        self.s3_creds = S3Credentials(
            s3_access_key = cdse_s3_access_key,
            s3_secret_key = cdse_s3_secret_key
        )


    def is_sh_creds_defined(self):
        return self.sh_creds.sh_clientid is not None \
            and self.sh_creds.sh_clientsecret is not None
    

    def is_s3_creds_defined(self):
        return self.s3_creds.s3_access_key is not None \
            and self.s3_creds.s3_secret_key is not None


@dataclasses.dataclass
class S3Path:
    bucket:str
    prefix:str

    def __hash__(self):
        return hash((self.bucket, self.prefix))
