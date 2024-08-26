
from NGPIris.parse_credentials import CredentialsHandler
from NGPIris.hcp.helpers import (
    raise_path_error,
    create_access_control_policy,
    check_mounted,
    download_folder_helper
)
from NGPIris.hcp.exceptions import (
    VPNConnectionError,
    BucketNotFound,
    ObjectAlreadyExist
)

from boto3 import client
from botocore.client import Config
from botocore.paginate import PageIterator, Paginator
from botocore.exceptions import EndpointConnectionError, ClientError
from boto3.s3.transfer import TransferConfig
from configparser import ConfigParser

from pathlib import Path
from os import (
    stat,
    listdir
)
from json import dumps
from parse import (
    parse,
    search,
    Result
)
from requests import get
from urllib3 import disable_warnings
from tqdm import tqdm
#from threading import Thread
from multiprocessing import Pool
from functools import partial

import time

from typing import Generator

_KB = 1024
_MB = _KB * _KB

class HCPHandler:
    def __init__(self, credentials_path : str, use_ssl : bool = False, proxy_path : str = "", custom_config_path : str = "") -> None:
        """
        Class for handling HCP requests.

        :param credentials_path: Path to the JSON credentials file
        :type credentials_path: str
        
        :param use_ssl: Boolean choice between using SSL, defaults to False
        :type use_ssl: bool, optional
        
        :param custom_config_path: Path to a .ini file for customs settings regarding download and upload
        :type custom_config_path: str, optional
        """
        credentials_handler = CredentialsHandler(credentials_path)
        self.hcp = credentials_handler.hcp
        self.endpoint = "https://" + self.hcp["endpoint"]
        tenant_parse = parse("https://{}.hcp1.vgregion.se", self.endpoint)
        if type(tenant_parse) is Result:
            self.tenant = str(tenant_parse[0])
        else: # pragma: no cover
            raise RuntimeError("Unable to parse endpoint. Make sure that you have entered the correct endpoint in your credentials JSON file. Hint: The endpoint should *not* contain \"https://\" or port numbers")
        self.base_request_url = self.endpoint + ":9090/mapi/tenants/" + self.tenant
        self.aws_access_key_id = self.hcp["aws_access_key_id"]
        self.aws_secret_access_key = self.hcp["aws_secret_access_key"]
        self.token = self.aws_access_key_id + ":" + self.aws_secret_access_key
        self.bucket_name = None
        self.use_ssl = use_ssl

        if not self.use_ssl:
            disable_warnings()

        if proxy_path: # pragma: no cover
            s3_config = Config(
                s3 = {
                    "addressing_style": "path",
                    "payload_signing_enabled": True
                },
                signature_version = "s3v4",
                proxies = CredentialsHandler(proxy_path).hcp
            )
        else:
            s3_config = Config(
                s3 = {
                    "addressing_style": "path",
                    "payload_signing_enabled": True
                },
                signature_version = "s3v4"
            )

        self.s3_client = client(
            "s3", 
            aws_access_key_id = self.aws_access_key_id, 
            aws_secret_access_key = self.aws_secret_access_key,
            endpoint_url = self.endpoint,
            verify = self.use_ssl,
            config = s3_config
        )

        if custom_config_path: # pragma: no cover
            ini_config = ConfigParser()
            ini_config.read(custom_config_path)

            self.transfer_config = TransferConfig(
                multipart_threshold = ini_config.getint("hcp", "multipart_threshold"),
                max_concurrency = ini_config.getint("hcp", "max_concurrency"),
                multipart_chunksize = ini_config.getint("hcp", "multipart_chunksize"),
                use_threads = ini_config.getboolean("hcp", "use_threads")
            )
        else:
            self.transfer_config = TransferConfig(
                multipart_threshold = 10 * _MB,
                max_concurrency = 60,
                multipart_chunksize = 40 * _MB,
                use_threads = True
            )
    
    def get_response(self, path_extension : str = "") -> dict:
        """
        Make a request to the HCP in order to use the builtin MAPI

        :param path_extension: Extension for the base request URL, defaults to the empty string
        :type path_extension: str, optional
        :return: The response as a dictionary
        :rtype: dict
        """
        url = self.base_request_url + path_extension
        headers = {
            "Authorization": "HCP " + self.token,
            "Cookie": "hcp-ns-auth=" + self.token,
            "Accept": "application/json"
        }
        response = get(
            url, 
            headers=headers,
            verify=self.use_ssl
        )

        response.raise_for_status()

        return dict(response.json())

    def test_connection(self, bucket_name : str = "") -> dict:
        """
        Test the connection to the mounted bucket or another bucket which is 
        supplied as the argument :py:obj:`bucket_name`.

        :param bucket_name: The name of the bucket to be mounted. Defaults to the empty string
        :type bucket_name: str, optional

        :raises RuntimeError: If no bucket is selected
        :raises VPNConnectionError: If there is no VPN connection
        :raises BucketNotFound: If no bucket of that name was found
        :raises Exception: Other exceptions

        :return: A dictionary of the response
        :rtype: dict
        """
        if not bucket_name and self.bucket_name:
            bucket_name = self.bucket_name
        elif bucket_name:
            pass
        else:
            raise RuntimeError("No bucket selected. Either use `mount_bucket` first or supply the optional `bucket_name` paramter for `test_connection`")
        try:
            response =  dict(self.s3_client.head_bucket(Bucket = bucket_name))
        except EndpointConnectionError as e: # pragma: no cover
            print(e)
            raise VPNConnectionError("Please check your connection and that you have your VPN enabled")
        except ClientError as e:
            print(e)
            raise BucketNotFound("Bucket \"" + bucket_name + "\" was not found")
        except Exception as e: # pragma: no cover
            raise Exception(e)
            
        if response["ResponseMetadata"].get("HTTPStatusCode", -1) != 200: # pragma: no cover
            error_msg = "The response code from the request made at " + self.endpoint + " returned status code " + response["ResponseMetadata"]["HTTPStatusCode"]
            raise Exception(error_msg)
        return response
        
    def mount_bucket(self, bucket_name : str) -> None:
        """
        Mount bucket that is to be used. This method needs to executed in order 
        for most of the other methods to work. It mainly concerns operations with 
        download and upload. 

        :param bucket_name: The name of the bucket to be mounted
        :type bucket_name: str
        """

        # Check if bucket exist
        self.test_connection(bucket_name = bucket_name)
        self.bucket_name = bucket_name

    def create_bucket(self, bucket_name : str) -> None:
        """
        Create a bucket. The user in the given credentials will be the owner 
        of the bucket

        :param bucket_name: Name of the new bucket
        :type bucket_name: str
        """
        self.s3_client.create_bucket(
            Bucket = bucket_name
        )

    def list_buckets(self) -> list[str]:
        """
        List all available buckets at endpoint.

        :return: A list of buckets
        :rtype: list[str]
        """
        
        response = self.get_response("/namespaces")
        list_of_buckets : list[str] = response["name"]
        return list_of_buckets
    
    @check_mounted
    def list_objects(self, path_key : str = "", name_only : bool = False) -> Generator:
        """
        List all objects in the mounted bucket as a generator. If one wishes to 
        get the result as a list, use :py:function:`list(list_objects())`

        :param path_key: Filter string for which keys to list, specifically for finding objects in certain folders.
        :type path_key: str, optional
        :param name_only: If True, yield only a the object names. If False, yield the full metadata about each object. Defaults to False.
        :type name_only: bool, optional
        :yield: A generator of all objects in a bucket
        :rtype: Generator
        """
        paginator : Paginator = self.s3_client.get_paginator("list_objects_v2")
        pages : PageIterator = paginator.paginate(Bucket = self.bucket_name)
        for object in pages.search("Contents[?starts_with(Key, '" + path_key + "')][]"):
            if name_only:
                yield str(object["Key"])
            else:
                yield object
                    
    @check_mounted
    def get_object(self, key : str) -> dict:
        """
        Retrieve object metadata

        :param key: The object name
        :type key: str

        :return: A dictionary containing the object metadata
        :rtype: dict
        """
        response = dict(self.s3_client.get_object(
            Bucket = self.bucket_name,
            Key = key
        ))
        return response

    @check_mounted
    def object_exists(self, key : str) -> bool:
        """
        Check if a given object is in the mounted bucket

        :param key: The object name
        :type key: str

        :return: True if the object exist, otherwise False
        :rtype: bool
        """
        try:
            response = self.get_object(key)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True
            else: # pragma: no cover
                return False
        except: # pragma: no cover
            return False

    @check_mounted
    def download_file(self, key : str, local_file_path : str) -> None:
        """
        Download one object file from the mounted bucket

        :param key: Name of the object
        :type key: str

        :param local_file_path: Path to a file on your local system where the contents of the object file can be put
        :type local_file_path: str
        """
        try:
            file_size : int = self.s3_client.head_object(Bucket = self.bucket_name, Key = key)["ContentLength"]
            with tqdm(
                total = file_size, 
                unit = "B", 
                unit_scale = True, 
                desc = key
            ) as pbar:
                self.s3_client.download_file(
                    Bucket = self.bucket_name, 
                    Key = key, 
                    Filename = local_file_path, 
                    Config = self.transfer_config,
                    Callback = lambda bytes_transferred : pbar.update(bytes_transferred)
                )
        except ClientError as e0: 
            print(str(e0))
            raise Exception("Could not find object", "\"" + key + "\"", "in bucket", "\"" + str(self.bucket_name) + "\"")
        except Exception as e: # pragma: no cover
            raise Exception(e)

    @check_mounted
    def download_folder(self, folder_key : str, local_folder_path : str) -> None:
        """
        Download multiple objects from a folder in the mounted bucket

        :param folder_key: Name of the folder
        :type folder_key: str

        :param local_folder_path: Path to a folder on your local system where the contents of the objects can be put
        :type local_folder_path: str

        :raises Exception: If local_folder_path is not a directory
        """
        #threads : list[Thread] = []
        if Path(local_folder_path).is_dir():
            _download_folder_helper_partial = partial(download_folder_helper, self, local_folder_path)
            with Pool() as pool:
                pool.map(_download_folder_helper_partial, self.list_objects(folder_key))
                
            #for object in self.list_objects(folder_key):
            #    p = Path(local_folder_path) / Path(object["Key"])
            #    if object["Key"][-1] == "/":
            #        p.mkdir()
            #    else:
            #        #thread = Thread(target = self.download_file, args = (object["Key"], p.as_posix()))
            #        #thread.start()
            #        #threads.append(thread)
            #        self.download_file(object["Key"], p.as_posix())
        else:
            raise Exception(local_folder_path + " is not a directory")
        
        #for thread in threads:
        #    thread.join()

    @check_mounted
    def upload_file(self, local_file_path : str, key : str = "") -> None:
        """
        Upload one file to the mounted bucket

        :param local_file_path: Path to the file to be uploaded
        :type local_file_path: str

        :param key: An optional new name for the file object on the bucket. Defaults to the same name as the file
        :type key: str, optional
        """
        raise_path_error(local_file_path)

        if not key:
            file_name = Path(local_file_path).name
            key = file_name

        if self.object_exists(key):
            raise ObjectAlreadyExist("The object \"" + key + "\" already exist in the mounted bucket")
        else:
            file_size : int = stat(local_file_path).st_size
            with tqdm(
                total = file_size, 
                unit = "B", 
                unit_scale = True, 
                desc = local_file_path
            ) as pbar:
                self.s3_client.upload_file(
                    Filename = local_file_path, 
                    Bucket = self.bucket_name, 
                    Key = key,
                    Config = self.transfer_config,
                    Callback = lambda bytes_transferred : pbar.update(bytes_transferred)
                )

    @check_mounted
    def upload_folder(self, local_folder_path : str, key : str = "") -> None:
        """
        Upload the contents of a folder to the mounted bucket

        :param local_folder_path: Path to the folder to be uploaded
        :type local_folder_path: str
        :param key: An optional new name for the folder path on the bucket. Defaults to the same name as the local folder path
        :type key: str, optional
        """
        raise_path_error(local_folder_path)

        if not key:
            key = local_folder_path
        filenames = listdir(local_folder_path)

        for filename in filenames:
            self.upload_file(local_folder_path + filename, key + filename)

    @check_mounted
    def delete_objects(self, keys : list[str], verbose : bool = True) -> None:
        """Delete a list of objects on the mounted bucket 

        :param keys: List of object names to be deleted
        :type keys: list[str]

        :param verbose: Print the result of the deletion. Defaults to True
        :type verbose: bool, optional
        """
        object_list = []
        for key in keys:
            object_list.append({"Key" : key})

        deletion_dict = {"Objects": object_list}

        list_of_objects_before = self.list_objects(name_only = True)

        response : dict = self.s3_client.delete_objects(
            Bucket = self.bucket_name,
            Delete = deletion_dict
        )
        if verbose:
            print(dumps(response, indent=4))
        diff : set[str] = set(keys) - set(list_of_objects_before)
        if diff:
            does_not_exist = []
            for key in diff:
                does_not_exist.append("- " + key + "\n")
            print("The following could not be deleted because they didn't exist: \n" + "".join(does_not_exist))
    
    @check_mounted
    def delete_object(self, key : str, verbose : bool = True) -> None:
        """
        Delete a single object in the mounted bucket

        :param key: The object to be deleted
        :type key: str
        :param verbose: Print the result of the deletion. Defaults to True
        :type verbose: bool, optional
        """
        self.delete_objects([key], verbose = verbose)

    @check_mounted
    def delete_folder(self, key : str, verbose : bool = True) -> None:
        """
        Delete a folder of objects in the mounted bucket. If there are subfolders, a RuntimeError is raisesd

        :param key: The folder of objects to be deleted
        :type key: str
        :param verbose: Print the result of the deletion. defaults to True
        :type verbose: bool, optional
        :raises RuntimeError: If there are subfolders, a RuntimeError is raisesd
        """
        if key[-1] != "/":
            key += "/"
        object_path_in_folder = []
        for s in self.search_objects_in_bucket(key):
            parse_object = parse(key + "{}", s)
            if type(parse_object) is Result:
                object_path_in_folder.append(s)

        for object_path in object_path_in_folder:
            if object_path[-1] == "/":
                raise RuntimeError("There are subfolders in this folder. Please remove these first, before deleting this one")
        self.delete_objects(object_path_in_folder + [key], verbose = verbose)

    @check_mounted
    def search_objects_in_bucket(self, search_string : str, case_sensitive : bool = False) -> list[str]:
        """
        Simple search method using substrings in order to find certain objects. Case insensitive by default. Does not utilise the HCI

        :param search_string: Substring to be used in the search
        :type search_string: str

        :param case_sensitive: Case sensitivity. Defaults to False
        :type case_sensitive: bool, optional

        :return: List of object names that match the in some way to the object names
        :rtype: list[str]
        """
        search_result : list[str] = []
        for key in self.list_objects(name_only = True):
            parse_object = search(
                search_string, 
                key, 
                case_sensitive = case_sensitive
            )
            if type(parse_object) is Result:
                search_result.append(key)
        return search_result

    @check_mounted
    def get_object_acl(self, key : str) -> dict:
        """
        Get the object Access Control List (ACL)

        :param key: The name of the object
        :type key: str

        :return: Return the ACL in the shape of a dictionary
        :rtype: dict
        """
        response : dict = self.s3_client.get_object_acl(
            Bucket = self.bucket_name,
            Key = key
        )
        return response

    @check_mounted
    def get_bucket_acl(self) -> dict:
        """
        Get the bucket Access Control List (ACL)

        :return: Return the ACL in the shape of a dictionary
        :rtype: dict
        """
        response : dict = self.s3_client.get_bucket_acl(
            Bucket = self.bucket_name
        )
        return response

    @check_mounted
    def modify_single_object_acl(self, key : str, user_ID : str, permission : str) -> None:
        """
        Modify permissions for a user in the Access Control List (ACL) for one object

        :param key: The name of the object
        :type key: str

        :param user_ID: The user name. Can either be the DisplayName or user_ID
        :type user_ID: str

        :param permission: 
            What permission to be set. Valid options are:
                * FULL_CONTROL 
                * WRITE 
                * WRITE_ACP 
                * READ 
                * READ_ACP\n
        :type permission: str
        """
        self.s3_client.put_object_acl(
            Bucket = self.bucket_name,
            Key = key,
            AccessControlPolicy = create_access_control_policy({user_ID : permission})
        )

    @check_mounted
    def modify_single_bucket_acl(self, user_ID : str, permission : str) -> None:
        """
        Modify permissions for a user in the Access Control List (ACL) for the mounted bucket

        :param user_ID: The user name. Can either be the DisplayName or user_ID
        :type user_ID: str
        
        :param permission: 
            What permission to be set. Valid options are: 
                * FULL_CONTROL 
                * WRITE 
                * WRITE_ACP 
                * READ 
                * READ_ACP\n
        :type permission: str
        """
        self.s3_client.put_bucket_acl(
            Bucket = self.bucket_name,
            AccessControlPolicy = create_access_control_policy({user_ID : permission})
        )

    @check_mounted
    def modify_object_acl(self, key_user_ID_permissions : dict[str, dict[str, str]]) -> None:
        """
        Modifies  permissions to multiple objects, see below.

        In order to add permissions for multiple objects, we make use of a dictionary of a dictionary: :py:obj:`key_user_ID_permissions = {key : {user_ID : permission}}`. So for every object (key), we set the permissions for every user ID for that object. 

        :param key_user_ID_permissions: The dictionary containing object name and user_id-permission dictionary
        :type key_user_ID_permissions: dict[str, dict[str, str]]
        """
        for key, user_ID_permissions in key_user_ID_permissions.items():
            self.s3_client.put_object_acl(
                Bucket = self.bucket_name,
                Key = key,
                AccessControlPolicy = create_access_control_policy(user_ID_permissions)
            )

    @check_mounted
    def modify_bucket_acl(self, user_ID_permissions : dict[str, str]) -> None:
        """
        Modify permissions for multiple users for the mounted bucket

        :param user_ID_permissions: The dictionary containing the user name and the corresponding permission to be set to that user
        :type user_ID_permissions: dict[str, str]
        """
        self.s3_client.put_bucket_acl(
            Bucket = self.bucket_name,
            AccessControlPolicy = create_access_control_policy(user_ID_permissions)
        )
