
from os import path as p
from typing import Callable, TypeVar, ParamSpec
from pathlib import Path
#from NGPIris.hcp import HCPHandler

from NGPIris.hcp.exceptions import NoBucketMounted

def create_access_control_policy(user_ID_permissions : dict[str, str]) -> dict:
    access_control_policy : dict[str, list] = {
        "Grants" : []
    }
    for user_ID, permission in user_ID_permissions.items():
        if not permission in ["FULL_CONTROL", "WRITE", "WRITE_ACP", "READ", "READ_ACP"]:
            print("Invalid permission option:", permission)
            exit()
        grantee = {
            "Grantee": {
                "ID": user_ID,
                "Type": "CanonicalUser"
            },
            "Permission": permission
        }
        access_control_policy["Grants"].append(grantee)
    return access_control_policy

def raise_path_error(path : str):
    if not p.exists(path):
        raise FileNotFoundError("\"" + path + "\"" + " does not exist")

T = TypeVar("T")
P = ParamSpec("P")

def check_mounted(method : Callable[P, T]) -> Callable[P, T]:
    def check_if_mounted(*args : P.args, **kwargs : P.kwargs) -> T:
        self = args[0]
        if not self.bucket_name: # type: ignore
            raise NoBucketMounted("No bucket is mounted")
        return method(*args, **kwargs)
    return check_if_mounted

def download_folder_helper(hcp_handler, local_folder_path : str, object : dict):
        p = Path(local_folder_path) / Path(object["Key"])
        if object["Key"][-1] == "/":
            p.mkdir()
        else:
            hcp_handler.download_file(object["Key"], p.as_posix())