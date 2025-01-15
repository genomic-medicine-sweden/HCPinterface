# NGP IRIS 👀
NGP IRIS, or just Iris, is a tool for interacting with a Hitachi Content Platform (HCP) using S3 in the `boto3` package. NGP Iris is designed with two use cases in mind:
* A simple, clear, real-time interaction with NGPr file management
* Improving process flow for performing off-site data analysis by using automated transfer scripts

Both of these cases can be achieved as either a [Python package](#as-a-python-package) or as a [Command Line Interface (CLI)](#as-a-command-line-tool).

## Getting started

### Prerequisites 
* Python 3.11.5[^1]
* NGPr credentials (see ["NGPr credentials"](#ngpr-credentials))

[^1]: Most versions of Python 3 should work

### Installation
Iris can be installed via PyPi by running the following:
```bash
pip install NGPIris
```

If you wish, you can also install Iris with the following steps:
1. Clone this repository
2. Open a terminal in your local copy of the repository
3. Run `pip install .`. This will install Iris along with the required Python packages in your Python environment

### NGPr credentials
In order to use Iris, a JSON file containing your credentials for the NGPr. The template of the JSON file can be found in [credentials/credentials_template.json](../credentials/credentials_template.json). Depending on your needs, you can either enter only the credentials for the HCP, only for the HCI ***or*** both. Do note that you can't leave some parts of either the HCP or HCI credentials empty:
```JSON
{
  "hcp" : {
      "endpoint" : "some_endpoint",
      "aws_access_key_id" : "",    
      "aws_secret_access_key" : "" 
  },
  "hci" : {
      "username" : "some_user",
      "password" : "some_password",
      "address" : "some_address",
      "auth_port" : "some_auth_port",
      "api_port" : "some_api_port"
  }
}
```
This will prompt Iris to complain about incomplete credentials (since the entries `aws_access_key_id` and `aws_secret_access_key` are empty). Of course, the same error would occur if the reverse between the HCP and HCI fields would be true.

**NOTE:** the `endpoint` field should not contain `https://` or any port number.

## Technical package documentation
A thorough package documentation can be found in the [technical documentation page]().

## Basic usage
Iris can be used as a Python package or by using the command line. The following sections cover some examples of how Iris might be used as a package and how to use its various commands. However, we highly recommend checking out the [tutorial](/docs/Tutorial.md) containing more example use cases.

### As a Python package
#### Connect to HCP
In order to connect to the HCP, we first need to create an `HCPHandler` object and mount it to some bucket:
```Python
from NGPIris.hcp import HCPHandler

hcp_h = HCPHandler("credentials.json")

hcp_h.mount_bucket("myBucket")
```
If you are unsure which buckets you are allowed to see, you can use `hcp_h.list_buckets()` in order to list all available buckets to you.

When you have successfully mounted a bucket, you can then do different operations onto the bucket. Object names on the bucket can be listed by typing `print(hcp_h.list_objects(True))`. 

##### Upload files
```Python
# Upload a single file to HCP
hcp_h.upload_file("myFile")

# Upload folder contents to HCP
hcp_h.upload_folder("myFiles/")
```

##### Download files
```Python
# Download a single object from HCP
hcp_h.download_file("myFile")
```

#### Connect to HCI
In order to connect to the HCI, we first need to create an `HCIHandler` object and request an authorization token:
```Python
from NGPIris.hci import HCIHandler

hci_h = HCIHandler("credentials.json")

hci_h.request_token()
```
Note that the token is stored inside of the `HCIHandler` object called `hci_h`. We can now request a list of indexes that are available by typing `print(hci_h.list_index_names())`. We can also look up information about a certain index with `print(hci_h.look_up_index("myIndex"))`. It is recommended to combine the use of the pretty print module `pprint` and the `json` module for this output, as it is mostly unreadable otherwise:
```Python
from NGPIris.hci import HCIHandler
from pprint import pprint
import json

hci_h = HCIHandler("credentials.json")

hci_h.request_token()

pprint(
    json.dumps(
        hci_h.look_up_index("myIndex"), 
        indent = 4
    )
)
```

### Miscellaneous utilities (`utils.py`)
The `utils` module can be contains two functions: one for converting a string to `base64` encoding and one for `MD5` encoding.

### As a command line tool
NGP Iris comes with two commands: `iris` and `iris_generate_credentials_file`. The latter command is used solely to generate the `.json` credentials file. Running `iris_generate_credentials_file --help` we get the following:
```
Usage: iris_generate_credentials_file [OPTIONS]

  Generate blank credentials file for the HCI and HCP.

  WARNING: This file will store sensisitve information (such as passwords) in
  plaintext.

Options:
  --path TEXT  Path for where to put the new credentials file
  --name TEXT  Custom name for the credentials file
  --help       Show this message and exit.
```

The `iris` command is used for communicating with the HCP and HCI. This includes upload and download to and from the HCP/HCI. Running `iris --help` yields the following:
```
Usage: iris [OPTIONS] CREDENTIALS COMMAND [ARGS]...

  NGP Intelligence and Repository Interface Software, IRIS.

  CREDENTIALS refers to the path to the JSON credentials file.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  delete-folder    Delete a folder from an HCP bucket/namespace.
  delete-object    Delete an object from an HCP bucket/namespace.
  download         Download files from an HCP bucket/namespace.
  list-buckets     List the available buckets/namespaces on the HCP.
  list-objects     List the objects in a certain bucket/namespace on the...
  simple-search    Make simple search using substrings in a...
  test-connection  Test the connection to a bucket/namespace.
  upload           Upload files to an HCP bucket/namespace.
```
Each sub-command has its own help message and is displayed by `iris path/to/your/credentials.json sub-command --help`

## Testing
Assuming that the repository has been cloned, run the following tests:
```shell
pytype
```
```shell
pytest
```
## Generating new documentation 
With force flag `-f`:
```shell
cd docs/
sphinx-apidoc ../NGPIris/ -o . -F -f 
```

Without force flag
```shell
cd docs/
sphinx-apidoc ../NGPIris/ -o . -F
```

## Compiling the documentation
```shell
cd docs/
make html
```