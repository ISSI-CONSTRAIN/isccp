import json
import os

import fsspec
import kerchunk.df
import kerchunk.hdf
import tqdm
from fsspec.implementations.reference import LazyReferenceMapper
from kerchunk.combine import MultiZarrToZarr


def create_references(fs, bucket_pattern, storage_options, fname):
    urls = collect_urls(fs, bucket_pattern)
    references = get_references(urls, storage_options)
    out = reference_fs(fname)
    combined_ref = concat_references(references, out)
    write_reference(combined_ref, fname)


def collect_urls(fs, bucket_pattern):
    """Recursively collect all files in bucket."""
    single_files = fs.glob(bucket_pattern)
    urls = ["s3://" + p for p in single_files]
    return urls


def get_references(urls, storage_options):
    """Collect metadata of files (path, filesize,...)"""
    references = []
    for u in tqdm.tqdm(urls):
        with fsspec.open(u, **storage_options) as inf:
            h5chunks = kerchunk.hdf.SingleHdf5ToZarr(inf, u, inline_threshold=100)
            references.append(h5chunks.translate())
    return references


def reference_fs(fname):
    """Create LazyReferenceMapper."""
    fs = fsspec.filesystem("file")
    if not os.path.exists(fname):
        os.makedirs(fname)
    out = LazyReferenceMapper.create(1000, fname, fs)
    return out


def concat_references(references, out):
    """Concatenate references to a single virtual zarr file."""
    mzz = MultiZarrToZarr(
        references,
        remote_protocol="s3",
        remote_options={"anon": True},
        concat_dims=["time"],
        out=out,
    )
    out_dict = mzz.translate()
    out.flush()
    return out_dict


def convert_json_to_parq(dictionary, fname):
    """Convert json reference file to parquet."""
    kerchunk.df.refs_to_dataframe(dictionary, fname)
    return


def write_reference(dictionary, fname, format="parquet"):
    """Write reference file to disk."""
    if not os.path.exists(os.path.dirname(fname)):
        os.makedirs(os.path.dirname(fname))
    if format == "json":
        with open(fname, "w") as fp:
            json.dump(dictionary, fp)
    elif format == "parquet":
        convert_json_to_parq(dictionary, fname)
    else:
        raise ValueError("Reference output format unknown.")
    return
