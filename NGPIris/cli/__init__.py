
import click
from click.core import Context
from json import dump
from pathlib import Path
from botocore.paginate import PageIterator, Paginator
from typing import Any, Generator
from os import get_terminal_size
from math import floor
from tabulate import tabulate

import time

from NGPIris.hcp import HCPHandler

def get_HCPHandler(context : Context) -> HCPHandler:
    return context.obj["hcph"]

def format_list(list_of_things : list) -> str:
    list_of_buckets = list(map(lambda s : s + "\n", list_of_things))
    return "".join(list_of_buckets).strip("\n")

def _list_objects_generator(hcph : HCPHandler, name_only : bool) -> Generator[str, Any, None]:
    paginator : Paginator = hcph.s3_client.get_paginator("list_objects_v2")
    pages : PageIterator = paginator.paginate(Bucket = hcph.bucket_name)
    (nb_of_cols, _) = get_terminal_size()
    max_width = floor(nb_of_cols / 5)
    if (not name_only):
        yield tabulate(
            [],
            headers = ["Key", "LastModified", "ETag", "Size", "StorageClass"],
            tablefmt = "plain",
            stralign = "center"
        ) + "\n" + "-"*nb_of_cols + "\n"
    for object in pages.search("Contents[?!ends_with(Key, '/')][]"): # filter objects that does not end with "/"
        if name_only:
            yield str(object["Key"]) + "\n"
        else:
            yield tabulate(
                [
                    [str(object["Key"]), 
                        str(object["LastModified"]), 
                        str(object["ETag"]), 
                        str(object["Size"]), 
                        str(object["StorageClass"])]
                ],
                maxcolwidths = max_width,
                tablefmt = "plain"
            ) + "\n" + "-"*nb_of_cols + "\n"

@click.group()
@click.argument("credentials")
@click.version_option(package_name = "NGPIris")
@click.pass_context
def cli(context : Context, credentials : str):
    """
    NGP Intelligence and Repository Interface Software, IRIS. 
    
    CREDENTIALS refers to the path to the JSON credentials file.
    """
    context.ensure_object(dict)
    context.obj["hcph"] = HCPHandler(credentials)

@cli.command()
@click.argument("bucket")
@click.argument("file-or-folder")
@click.argument("object_path")
@click.pass_context
def upload(context : Context, bucket : str, file_or_folder : str, object_path : str):
    """
    Upload files to an HCP bucket/namespace. 
    
    BUCKET is the name of the upload destination bucket.

    FILE-OR-FOLDER is the path to the file or folder of files to be uploaded.
    
    OBJECT-PATH is the destination path on the HCP. 
    """
    hcph : HCPHandler = get_HCPHandler(context)
    hcph.mount_bucket(bucket)
    if Path(file_or_folder).is_dir():
        hcph.upload_folder(file_or_folder, object_path)
    else:
        hcph.upload_file(file_or_folder, object_path)

@cli.command()
@click.argument("bucket")
@click.argument("object_path")
@click.argument("local_path")
@click.option(
    "-f", 
    "--force", 
    help = "Overwrite existing file with the same name", 
    is_flag = True
)
@click.pass_context
def download_file(context : Context, bucket : str, object_path : str, local_path : str, force : bool):
    """
    Download a file from an HCP bucket/namespace.

    BUCKET is the name of the download source bucket.

    OBJECT_PATH is the path to the object to be downloaded.

    LOCAL_PATH is the folder where the downloaded object is to be stored locally.
    """
    if not Path(local_path).exists():
        Path(local_path).mkdir()
    
    downloaded_object_path = Path(local_path) / Path(object_path).name
    if downloaded_object_path.exists() and not force:
        exit("Object already exists. If you wish to overwrite the existing file, use the -f, --force option")
    hcph : HCPHandler = get_HCPHandler(context)
    hcph.mount_bucket(bucket)
    hcph.download_file(object_path, downloaded_object_path.as_posix())

@cli.command()
@click.argument("bucket")
@click.argument("folder_path")
@click.argument("local_path")
@click.pass_context
def download_folder(context : Context, bucket : str, folder_path : str, local_path : str):
    """
    Download a folder from an HCP bucket/namespace.

    BUCKET is the name of the download source bucket.

    FOLDER_PATH is the path to the object folder to be downloaded.

    LOCAL_PATH is the folder where the downloaded object folder is to be stored locally.
    """
    if not Path(local_path).exists():
        Path(local_path).mkdir()
    
    hcph : HCPHandler = get_HCPHandler(context)
    hcph.mount_bucket(bucket)
    #start = time.time()
    if folder_path == "/":
        folder_path = ""
    hcph.download_folder(folder_path, Path(local_path).as_posix())
    #duration = time.time() - start
    #click.echo("Time: " + str(duration) + " seconds")

@cli.command()
@click.argument("bucket")
@click.argument("object")
@click.pass_context
def delete_object(context : Context, bucket : str, object : str):
    """
    Delete an object from an HCP bucket/namespace. 

    BUCKET is the name of the bucket where the object to be deleted exist.

    OBJECT is the name of the object to be deleted.
    """
    hcph : HCPHandler = get_HCPHandler(context)
    hcph.mount_bucket(bucket)
    hcph.delete_object(object)

@cli.command()
@click.argument("bucket")
@click.argument("folder")
@click.pass_context
def delete_folder(context : Context, bucket : str, folder : str):
    """
    Delete a folder from an HCP bucket/namespace. 

    BUCKET is the name of the bucket where the folder to be deleted exist.

    FOLDER is the name of the folder to be deleted.
    """
    hcph : HCPHandler = get_HCPHandler(context)
    hcph.mount_bucket(bucket)
    hcph.delete_folder(folder)

@cli.command()
@click.pass_context
def list_buckets(context : Context):
    """
    List the available buckets/namespaces on the HCP.
    """
    hcph : HCPHandler = get_HCPHandler(context)
    click.echo(format_list(hcph.list_buckets()))

@cli.command()
@click.argument("bucket")
@click.option(
    "-no", 
    "--name-only", 
    help = "Output only the name of the objects instead of all the associated metadata", 
    default = False
)
@click.pass_context
def list_objects(context : Context, bucket : str, name_only : bool):
    """
    List the objects in a certain bucket/namespace on the HCP.

    BUCKET is the name of the bucket in which to list its objects.
    """
    hcph : HCPHandler = get_HCPHandler(context)
    hcph.mount_bucket(bucket)
    click.echo_via_pager(_list_objects_generator(hcph, name_only))

@cli.command()
@click.argument("bucket")
@click.argument("search_string")
@click.option(
    "-cs", 
    "--case_sensitive", 
    help = "Use case sensitivity? Default value is False", 
    default = False
)
@click.pass_context
def simple_search(context : Context, bucket : str, search_string : str, case_sensitive : bool):
    """
    Make simple search using substrings in a bucket/namespace on the HCP.

    BUCKET is the name of the bucket in which to make the search.

    SEARCH_STRING is any string that is to be used for the search.
    """
    hcph : HCPHandler = get_HCPHandler(context)
    hcph.mount_bucket(bucket)
    list_of_results = hcph.search_objects_in_bucket(search_string, case_sensitive)
    click.echo("Search results:")
    for result in list_of_results:
        click.echo("- " + result)

@cli.command()
@click.argument("bucket")
@click.pass_context
def test_connection(context : Context, bucket : str):
    """
    Test the connection to a bucket/namespace.

    BUCKET is the name of the bucket for which a connection test should be made.
    """
    hcph : HCPHandler = get_HCPHandler(context)
    click.echo(hcph.test_connection(bucket))

@click.command()
@click.option(
    "--path",
    help = "Path for where to put the new credentials file.",
    default = ""
)
@click.option(
    "--name",
    help = "Custom name for the credentials file. Will filter out everything after a \".\" character, if any exist.",
    default = "credentials"
)
def iris_generate_credentials_file(path : str, name : str):
    """
    Generate blank credentials file for the HCI and HCP. 

    WARNING: This file will store sensitive information (such as passwords) in plaintext.
    """
    credentials_dict = {
        "hcp" : {
            "endpoint" : "",
            "aws_access_key_id" : "",
            "aws_secret_access_key" : ""
        },
        "hci" : {
            "username" : "",
            "password" : "",
            "address" : "",
            "auth_port" : "",
            "api_port" : ""
        }
    }

    name = name.split(".")[0] + ".json"
    if path:
        if not path[-1] == "/":
            path += "/"

        if path == ".":
            file_path = name    
        else:
            file_path = path + name
    
        if not Path(path).is_dir():
            Path(path).mkdir()
    else:
        file_path = name
        
    with open(file_path, "w") as f:
        dump(credentials_dict, f, indent = 4)

    