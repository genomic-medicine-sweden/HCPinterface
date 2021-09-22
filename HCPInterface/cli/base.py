import click
import logging
import sys

from HCPInterface import log, logformat
from HCPInterface.hcp import HCPManager
from HCPInterface.hci import hci
from HCPInterface.preproc import preproc
from HCPInterface.cli.functions import delete, search, upload, download
from HCPInterface.cli.hci_functions import hci

@click.group()
@click.option('-c',"--credentials", help="File containing ep, id & key",type=click.Path(),required=True)
@click.option("-b","--bucket",help="Bucket name",type=str,required=True)
@click.option("-ep","--endpoint",help="Endpoint URL override",type=str,default="")
@click.option("-id","--access_key_id",help="Amazon key identifier override",type=str,default="")
@click.option("-key","--access_key",help="Amazon secret access key override",type=str,default="")
@click.option("-l","--logfile",type=click.Path(),help="Logs activity to provided file",default="")
@click.version_option()
@click.pass_context
def root(ctx, endpoint, access_key_id, access_key, bucket, credentials,logfile):
    """HCP interfacing tool"""
    [ep, aid, key] = preproc.read_credentials(credentials)

    if endpoint != "":
      ep = endpoint
    if access_key_id != "":
      aid = access_key_id
    if access_key != "":
      key = access_key

    ctx.obj = {}
    hcpm = HCPManager(ep, aid, key, bucket=bucket)
    hcpm.attach_bucket(bucket)
    ctx.obj["hcpm"] = hcpm

    if logfile != "":
        fh = logging.FileHandler(logfile)
        fh.setFormatter(logformat)
        log.addHandler(fh)


root.add_command(hci)
root.add_command(delete)
root.add_command(search)
root.add_command(upload)
root.add_command(download)
