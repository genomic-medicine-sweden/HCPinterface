
from configparser import ConfigParser
from NGPIris.hci import HCIHandler
from random import randint
from json import dump
from os import remove

ini_config = ConfigParser()
ini_config.read("tests/test_conf.ini")

hci_h = HCIHandler(ini_config.get("General", "credentials_path"))
hci_h.request_token()

def test_list_index_names_type() -> None:
    list_of_indexes = hci_h.list_index_names()
    assert type(list_of_indexes) is list

def test_look_up_all_indexes() -> None:
    list_of_indexes = hci_h.list_index_names()
    for index in list_of_indexes:
        assert hci_h.look_up_index(index)

def test_fail_index_look_up() -> None:
    assert not hci_h.look_up_index("anIndexThatDoesNotExist")
    
def test_make_simple_raw_query() -> None:
    list_of_indexes = hci_h.list_index_names()
    arbitrary_index = list_of_indexes[randint(0, len(list_of_indexes) - 1)]
    query = {
        "indexName" : arbitrary_index
    }
    result = hci_h.raw_query(query)
    assert result["indexName"] == arbitrary_index

def test_raw_query_with_results() -> None:
    query = {
        "indexName" : "Parsed_VCF_Index",
        "facetRequests" : [
            {
                "fieldName" : "ref"
            },
            {
                "fieldName" : "alt"
            }
        ]
    }
    parsed_VCF_index_result : list = hci_h.raw_query(query)["facets"]
    for i in range(len(parsed_VCF_index_result)):
        assert parsed_VCF_index_result[i]["termCounts"] # Assert if the result is not empty

def test_fail_raw_query() -> None:
    query = {}
    try:
        hci_h.raw_query(query)
    except:
        assert True
    else: # pragma: no cover
        assert False

def test_make_simple_raw_query_from_JSON() -> None:
    list_of_indexes = hci_h.list_index_names()
    arbitrary_index = list_of_indexes[randint(0, len(list_of_indexes) - 1)]
    path = "tests/data/json_test_query.json"
    with open(path, "w") as f:
        query = {
            "indexName": arbitrary_index
        }
        dump(query, f, indent = 4)
    result = hci_h.raw_query_from_JSON(path)
    assert result["indexName"] == arbitrary_index
    remove(path)    
    