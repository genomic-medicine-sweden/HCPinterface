
from NGPIris.hcp import HCPHandler
from configparser import ConfigParser
from os import mkdir, rmdir, remove, listdir
from filecmp import cmp

hcp_h = HCPHandler("credentials/testCredentials.json")

ini_config = ConfigParser()
ini_config.read("tests/test_conf.ini")

test_bucket = ini_config.get("hcp_tests", "bucket")

test_file = ini_config.get("hcp_tests","data_test_file")
test_file_path = "tests/data/" + test_file

result_path = "tests/data/results/"

def test_list_buckets() -> None:
    assert hcp_h.list_buckets()

def test_mount_bucket() -> None:
    hcp_h.mount_bucket(test_bucket)

def test_mount_nonexisting_bucket() -> None:
    try:
        hcp_h.mount_bucket("aBucketThatDoesNotExist")
    except:
        assert True
    else: # pragma: no cover
        assert False

def test_list_objects() -> None:
    test_mount_bucket()
    assert type(hcp_h.list_objects()) == list

def test_upload_file() -> None:
    test_mount_bucket()
    hcp_h.upload_file(test_file_path)

def test_upload_nonexistent_file() -> None:
    test_mount_bucket()
    try: 
        hcp_h.upload_file("tests/data/aTestFileThatDoesNotExist")
    except:
        assert True
    else: # pragma: no cover
        assert False

def test_upload_folder() -> None:
    test_mount_bucket()
    hcp_h.upload_folder("tests/data/a folder of data/")

def test_upload_nonexisting_folder() -> None:
    test_mount_bucket()
    try: 
        hcp_h.upload_folder("tests/data/aFolderOfFilesThatDoesNotExist")
    except:
        assert True
    else: # pragma: no cover
        assert False

def test_get_file() -> None:
    test_mount_bucket()
    assert hcp_h.object_exists(test_file)
    assert hcp_h.get_object(test_file)

def test_download_file() -> None:
    test_mount_bucket()
    mkdir(result_path)
    hcp_h.download_file(test_file, result_path + test_file)
    assert cmp(result_path + test_file, test_file_path)

def test_download_nonexistent_file() -> None:
    test_mount_bucket()
    try:
        hcp_h.download_file("aFileThatDoesNotExist", result_path + "aFileThatDoesNotExist")
    except:
        assert True
    else: # pragma: no cover
        assert False

def test_search_objects_in_bucket() -> None:
    test_mount_bucket()
    hcp_h.search_objects_in_bucket(test_file)

def test_get_object_acl() -> None:
    test_mount_bucket()
    hcp_h.get_object_acl(test_file)

def test_get_bucket_acl() -> None:
    test_mount_bucket()
    hcp_h.get_bucket_acl()

#def test_modify_single_object_acl() -> None:
#    test_mount_bucket()
#    hcp_h.modify_single_object_acl()
#
#def test_modify_single_bucket_acl() -> None:
#    test_mount_bucket()
#    hcp_h.modify_single_bucket_acl()
#
#def test_modify_object_acl() -> None:
#    test_mount_bucket()
#    hcp_h.modify_object_acl()
#
#def test_modify_bucket_acl() -> None:
#    test_mount_bucket()
#    hcp_h.modify_bucket_acl()

def test_delete_file() -> None:
    test_mount_bucket()
    hcp_h.delete_object(test_file)
    for file in listdir("tests/data/a folder of data/"):
        hcp_h.delete_object(file)

def test_delete_nonexistent_files() -> None:
    hcp_h.delete_objects(["some", "files", "that", "does", "not", "exist"])

def test_clean_up() -> None:
    remove(result_path + test_file)
    rmdir(result_path)