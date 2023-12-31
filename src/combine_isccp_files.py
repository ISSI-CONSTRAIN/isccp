"""Combine ISCCP files of single timesteps to one single dataset by creating a
kerchunk reference file.

Usage of reference file:

import xarray as xr
ds = xr.open_dataset(
    "reference://", engine="zarr",
    backend_kwargs={
        "storage_options": {
            "fo": out,
            "remote_protocol": "s3",
            "remote_options": {"anon": True}
        },
        "consolidated": False
    }
)
"""
import argparse
import sys

import s3fs

sys.path.append("./src/")
import kerchunk_helpers as kh  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument(
    "--product_family",
    help="Product family. Allowed options: isccp-basic, isccp.",
)
parser.add_argument(
    "--product",
    help=(
        "Product. Allowed options: hgh (H-series gridded monthly by hour), hgg (H-series"
        " gridded global), hgm (H-series gridded monthly), hxg (H-series pixel level"
        " gridded)."
    ),
)
parser.add_argument(
    "--yyyymm",
    help="Glob pattern to select subsection of dataset.",
    default="*",
)
parser.add_argument(
    "--reference",
    help="Reference filename to write output to.",
)
args = parser.parse_args()

s3 = s3fs.S3FileSystem(anon=True)

product_family = args.product_family  # isccp-basic, isccp
product = args.product  # isccp-basic: hgh, hgg, hgm, isccp: hgh, hgg, hgm, hxg
reference_fn = args.reference
YYYYmm = args.yyyymm
bucket = "s3://noaa-cdr-cloud-properties-isccp-pds"
bucket_fmt = {
    "hgh": f"/data/{product_family}/{product}/{YYYYmm}/*.nc",
    "hgm": f"/data/{product_family}/{product}/*{YYYYmm[:4]}.{YYYYmm[4:]}*.nc",
    "hgg": f"/data/{product_family}/{product}/{YYYYmm}/*.nc",
    "hxg": f"/data/{product_family}/{product}/{YYYYmm}/*.nc",
}
bucket_pattern = bucket + bucket_fmt[product]
storage_options = {
    "anon": True,
    "default_fill_cache": False,
    "default_cache_type": "first",
}

kh.create_references(s3, bucket_pattern, storage_options, reference_fn)
