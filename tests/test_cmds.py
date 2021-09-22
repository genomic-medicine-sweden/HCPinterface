#!/usr/bin/env python

import builtins
import click
import json
import logging
import pathlib
import pdb
import pytest
import re
import mock
import os
import sys

from HCPInterface import log, WD
from HCPInterface.cli.base import root

from click.testing import CliRunner
from distutils.sysconfig import get_python_lib
from unittest.mock import patch, mock_open

testWD = os.path.join(WD, '..', 'tests')
credentials_path = os.path.join(testWD, 'credentials.json')
f1target =  os.path.join("unittest","test_reads_R1.fastq.gz") 


@pytest.fixture
def runner():
    runnah = CliRunner()
    return runnah

def test_version(runner):
    res = runner.invoke(root, '--version')
    assert res.exit_code == 0

def test_base(runner):
    cmd = f"-b ngs-test -c {credentials_path}"
    res = runner.invoke(root, cmd.split())
    #Command should complain about lack of subcommand
    assert res.exit_code == 2

def test_hci_base(runner):
    cmd = f"-b ngs-test -c {credentials_path} hci"
    res = runner.invoke(root, cmd.split())
    assert res.exit_code == 0

def test_upload(runner):
    source = os.path.join(testWD,"data","test_reads_R1.fastq.gz")

    cmd = f"-b ngs-test -c {credentials_path} upload -i {source} -d {f1target} -m /tmp/meta.json --silent"
    #import pdb; pdb.set_trace()
    log.debug(cmd)
    res = runner.invoke(root, cmd.split())
    assert res.exit_code == 0

def test_search(runner):
    cmd = "-b ngs-test -c {0} search -q {1}".format(credentials_path, f1target)
    res = runner.invoke(root, cmd.split())
    assert res.exit_code == 0 

def test_download(runner):
    dest =  os.path.join('tmp','tst.fq')
    cmd = f"-b ngs-test -c {credentials_path} download -f -q {f1target} -d /{dest} --silent"
    log.debug(cmd)
    res = runner.invoke(root, cmd.split())
    assert res.exit_code == 0

def test_delete(runner):
    cmd1 = f"-b ngs-test -c {credentials_path} delete -q {f1target} -f"
    res1 = runner.invoke(root, cmd1.split())
    #assert res.exit_code == 0
    cmd2 = f"-b ngs-test -c {credentials_path} delete -q {os.path.join('unittest','meta.json')} -f"
    res2 = runner.invoke(root, cmd2.split())
    assert res1.exit_code == 0
    assert res2.exit_code == 0
