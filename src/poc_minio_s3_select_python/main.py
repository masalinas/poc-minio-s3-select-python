"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following lines in the
``[options.entry_points]`` section in ``setup.cfg``::

    console_scripts =
         s3_select = poc_minio_s3_select_python.main:run

Then run ``pip install .`` (or ``pip install -e .`` for editable mode)
which will install the command ``s3_select`` inside your current environment.

Besides console scripts, the header (i.e. until ``_logger``...) of this file can
also be used as template for Python modules.

Note:
    This file can be renamed depending on your needs or safely removed if not needed.

References:
    - https://setuptools.pypa.io/en/latest/userguide/entry_point.html
    - https://pip.pypa.io/en/stable/reference/pip_install
"""

import argparse
import logging
import sys
import boto3
import pandas as pd
from io import StringIO

from poc_minio_s3_select_python import __version__

__author__ = "Miguel Salinas Gancedo"
__copyright__ = "Miguel Salinas Gancedo"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


# ---- Python API ----
# The functions defined in this section can be imported by users in their
# Python scripts/interactive interpreter, e.g. via
# `from poc_minio_s3_select_python.skeleton import fib`,
# when using this Python module as a library.

s3 = boto3.client('s3',
                  endpoint_url='https://localhost:9000',
                  aws_access_key_id='gl8rbGORHSpxmg1V',
                  aws_secret_access_key='8WphDMckYqRb29s43SzA4trsV2GgaQRc',
                  verify=False,
                  region_name='us-east-1')

def get_expressions_by_annotations(s3, annotation_id):    
    r = s3.select_object_content(
        Bucket='65cd021098d02623c46da92d',
        Key='65cd02d9e6ba3947be825ac8/66085488056b08fae55840e5/gen_datamatrix.csv',
        ExpressionType='SQL',
        Expression='select s._1, s.\"' + annotation_id + '\" from s3object s',
        InputSerialization={
            'CSV': {
                "FileHeaderInfo": "USE",
            },
            'CompressionType': 'NONE',
        },
        OutputSerialization={'CSV': {}},
    )

    expressions = None
    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')  

            df =  pd.read_csv(StringIO(records), header=None)
            
            if expressions is None:
                expressions = df
            else:            
                expressions = pd.concat([expressions, df])

        elif 'End' in event:
            print("End Event")
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            print("Stats details bytesScanned: ")
            print(statsDetails['BytesScanned'])
            print("Stats details bytesProcessed: ")
            print(statsDetails['BytesProcessed'])

    print(expressions)

    return None


# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(description="Just a Fibonacci demonstration")
    parser.add_argument(
        "--version",
        action="version",
        version=f"poc-minio-s3-select-python {__version__}",
    )
    parser.add_argument(
        "-annotation-id",
        "--annotation-id",
        dest="annotation_id",
        help="Annotation Id"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args):
    """Wrapper allowing :func:`fib` to be called with string arguments in a CLI fashion

    Instead of returning the value from :func:`fib`, it prints the result to the
    ``stdout`` in a nicely formatted message.

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)

    _logger.debug("Starting get expressions by annotations by ...")
    annotation_id = args.annotation_id
    get_expressions_by_annotations(s3, annotation_id)

    _logger.info("Script ends here")


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`

    This function can be used as entry point to create console scripts with setuptools.
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    # ^  This is a guard statement that will prevent the following code from
    #    being executed in the case someone imports this file instead of
    #    executing it as a script.
    #    https://docs.python.org/3/library/__main__.html

    # After installing your project with pip, users can also run your Python
    # modules as scripts via the ``-m`` flag, as defined in PEP 338::
    #
    #     python -m poc_minio_s3_select_python.skeleton 42
    #
    run()
