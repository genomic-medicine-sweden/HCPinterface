# NGP IRIS 5 Tutorial
---
- [Introduction](#introduction)
- [CLI](#cli)
  - [The `iris` command](#the-iris-command)
    - [Example use cases](#example-use-cases)
      - [Listing buckets/namespaces](#listing-bucketsnamespaces)
      - [Downloading a file](#downloading-a-file)
      - [Uploading a file](#uploading-a-file)
      - [Searching for a file](#searching-for-a-file)
      - [Delete a file](#delete-a-file)
      - [Delete a folder](#delete-a-folder)
  - [The `iris_generate_credentials_file` command](#the-iris_generate_credentials_file-command)
- [Package](#package)
  - [The `HCPHandler` class](#the-hcphandler-class)
    - [Example use cases](#example-use-cases-1)
      - [Listing buckets/namespaces](#listing-bucketsnamespaces-1)
      - [Listing objects in a bucket/namespace](#listing-objects-in-a-bucketnamespace)
      - [Downloading a file](#downloading-a-file-1)
      - [Uploading a file](#uploading-a-file-1)
      - [Uploading a folder](#uploading-a-folder)
      - [Searching for a file](#searching-for-a-file-1)
      - [Delete a file](#delete-a-file-1)
      - [Delete a folder](#delete-a-folder-1)
  - [The `HCIHandler` class](#the-hcihandler-class)
    - [Example use cases](#example-use-cases-2)
      - [List index names](#list-index-names)
      - [Look up information of an index](#look-up-information-of-an-index)
      - [Make queries](#make-queries)
---

This tutorial was updated for `NGPIris 5.1`.

## Introduction
IRIS 5 is a complete overhaul of the previous versions of IRIS, mainly in terms of its codebase. The general functionality like download from and upload to the HCP are still here, but might differ from previous versions from what you are used to. This document will hopefully shed some light on what users can expect and how your workflow with IRIS might change in comparison to previous versions of IRIS. 

IRIS 5, like previous versions of IRIS, consists of two main parts: a Python package and an associated Command Line Interface (CLI), which are described below. 

## CLI 
IRIS 5 features a CLI like recent versions of IRIS. However, the new CLI is a bit different compared to before; the general structure of subcommands for the `iris` command are vastly different, but it still has the subcommands you would come to expect. A new, separate, command called `iris_generate_credentials_file` has also been added. It will generate an empty credentials file that can be filled in with your own NGPr credentials. 

### The `iris` command

Typing `iris --help` yields the following:
```cmd
Usage: iris [OPTIONS] CREDENTIALS COMMAND [ARGS]...

  NGP Intelligence and Repository Interface Software, IRIS.

  CREDENTIALS refers to the path to the JSON credentials file.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  delete-folder    Delete a folder from an HCP bucket/namespace.
  delete-object    Delete an object from an HCP bucket/namespace.
  download         Download a file or folder from an HCP bucket/namespace.
  list-buckets     List the available buckets/namespaces on the HCP.
  list-objects     List the objects in a certain bucket/namespace on the...
  simple-search    Make simple search using substrings in a...
  test-connection  Test the connection to a bucket/namespace.
  upload           Upload files to an HCP bucket/namespace.
```
* `delete-folder`: Deletes a folder on the HCP
* `delete-object`: Deletes an object on the HCP
* `download`: Downloads a file or folder from a bucket/namespace on the HCP
* `list-buckets`: Lists all buckets that the user is allowed to see
* `list-objects`: Lists all objects that the user is allowed to see
* `simple-search`: Performs a simple search using a substring in order to find matching objects in a bucket/namespace
* `test-connection`: Used for testing your connection to the HCP
* `upload`: Uploads a file or a folder to a bucket/namespace on the HCP

#### Example use cases
The following subsections contain examples of simple use cases for IRIS 5. Of course, correct paths and bucket names should be replaced for your circumstances.
##### Listing buckets/namespaces
```shell
iris path/to/your/credentials.json list-buckets
```
##### Downloading a file
```shell
iris path/to/your/credentials.json download the_name_of_the_bucket path/to/your/file/in/the/bucket path/on/your/local/machine
```
##### Uploading a file
```shell
iris path/to/your/credentials.json upload the_name_of_the_bucket destination/path/in/the/bucket path/to/your/file/on/your/local/machine
```
##### Searching for a file
By default, the `simple-search` command is case insensitive:
```shell
iris path/to/your/credentials.json simple-search the_name_of_the_bucket your_search_string
```
This can be changed with the `--case_sensitive` option:
```shell
iris path/to/your/credentials.json simple-search --case_sensitive True the_name_of_the_bucket your_case_sensitive_search_string
```
##### Delete a file
```shell
iris path/to/your/credentials.json delete-object the_name_of_the_bucket path/to/your/file/in/the/bucket
```
##### Delete a folder
```shell
iris path/to/your/credentials.json delete-folder the_name_of_the_bucket path/to/your/folder/on/the/bucket/
```

### The `iris_generate_credentials_file` command
IRIS 5 comes with a new separate command for generating your NGPr credentials: `iris_generate_credentials_file`. The idea with this command is to make it easier for anyone to ensure the correct structure of their credentials file. Typing `iris_generate_credentials_file --help` yields the following:
```cmd
Usage: iris_generate_credentials_file [OPTIONS]

  Generate blank credentials file for the HCI and HCP.

  WARNING: This file will store sensitive information (such as passwords) in
  plaintext.

Options:
  --path TEXT  Path for where to put the new credentials file.
  --name TEXT  Custom name for the credentials file. Will filter out
               everything after a "." character, if any exist.
  --help       Show this message and exit.
```
Simply running `iris_generate_credentials_file` will generate a blank credentials file (which is just a JSON file) like the following:
```json
{
  "hcp": {
      "endpoint": "",
      "aws_access_key_id": "",
      "aws_secret_access_key": ""
  },
  "hci": {
      "username": "",
      "password": "",
      "address": "",
      "auth_port": "",
      "api_port": ""
  }
}
```

## Package
The updated codebase for IRIS 5 contains some major changes to use of the package, but should still be familiar. The use cases of IRIS 5 is sill intended to be the same as in previous versions. The difference between IRIS 5 and previous versions is the new syntax and names of classes, methods and functions. Everything in IRIS 5 was inspired by the previous implementations of the `boto3` library, which means that most functionality should still exist, but in a different form; methods and functions may have new names, and they might be combined or separated.

### The `HCPHandler` class 
IRIS 5 comes with a class for handling connections with the HCP:
```python
HCPHandler(
  self, 
  credentials_path : str, 
  use_ssl : bool = False, 
  proxy_path : str = "", 
  custom_config_path : str = ""
)
```
Generally, a bucket must be mounted before any methods can be executed. If not, IRIS will throw a `NoBucketMounted` error. A bucket can be mounted with the following code:
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")
```

#### Example use cases
##### Listing buckets/namespaces
There is no need for mounting a bucket when listing all available buckets. However, credentials are still needed. As such, we can list all buckets with the following:
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

print(hcp_h.list_buckets())
```
##### Listing objects in a bucket/namespace
Since there might be many objects in a given bucket, a regular Python list would be memory inefficient. As such, a `Generator` is returned instead. Since `Generator`s are lazy objects, if we want to explicitly want all the objects we must first cast it to a `list`
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")

objects_generator = hcp_h.list_objects()

print(list(objects_generator))
```

##### Downloading a file
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")

hcp_h.download_file("path/to/object/in/bucket", "path/on/local/machine")
```
##### Uploading a file
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")

hcp_h.upload_file("path/on/local/machine", "path/to/object/in/bucket")
```
##### Uploading a folder
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")

hcp_h.upload_folder("path/on/local/machine/", "path/to/folder/in/bucket/")
```
##### Searching for a file
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")

print(hcp_h.search_objects_in_bucket("a search string"))
```
##### Delete a file
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")

hcp_h.delete_object("path/to/object/in/bucket/")
```
##### Delete a folder
```python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")

hcp_h.delete_folder("path/to/folder/in/bucket/")
```
### The `HCIHandler` class 
IRIS 5 comes with a class for handling connections with the HCI:
```python
HCIHandler(
  self, 
  credentials_path : str, 
  use_ssl : bool = False
)
```
A token can be requested to the handler, which in turn allows us to make requests to the HCI:
```python
from NGPIris.hci import HCIHandler

hci_h = HCIHandler("credentials.json")

hci_h.request_token()
```
#### Example use cases
##### List index names
```python
from NGPIris.hci import HCIHandler

hci_h = HCIHandler("credentials.json")
hci_h.request_token()

print(hci_h.list_index_names())
```
##### Look up information of an index
```python
from NGPIris.hci import HCIHandler
from pprint import pprint
from json import dumps

hci_h = HCIHandler("credentials.json")
hci_h.request_token()

pprint(
    dumps(
        hci_h.look_up_index("myIndex"), # A dictionary is returned, so we use 
        indent = 4                      # dumps and pprint in order to make the 
    )                                   # output more readable
)
```
##### Make queries
```python
from NGPIris.hci import HCIHandler
from pprint import pprint
from json import dumps

hci_h = HCIHandler("credentials.json")
hci_h.request_token()

query = {
    "indexName" : "myIndex",
    "facetRequests" : [
        {
            "fieldName" : "ref"
        },
        {
            "fieldName" : "alt"
        }
    ]
}
pprint(
    dumps(
        hci_h.raw_query(query), # A dictionary is returned, so we use 
        indent = 4              # dumps and pprint in order to make the 
    )                           # output more readable
)
```