import json
import os

import fsspec
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr


def collect_urls(fs, bucket_pattern):
    single_files = fs.glob(bucket_pattern)
    urls = ["s3://" + p for p in single_files]
    return urls


def get_references(urls, storage_options):
    references = []
    for u in urls:
        with fsspec.open(u, **storage_options) as inf:
            h5chunks = kerchunk.hdf.SingleHdf5ToZarr(inf, u, inline_threshold=100)
            references.append(h5chunks.translate())
    return references


def concat_references(references):
    mzz = MultiZarrToZarr(
        references,
        remote_protocol="s3",
        remote_options={"anon": True},
        concat_dims=["time"],
    )
    out = mzz.translate()
    return out


def write_reference(dictionary, fname):
    """Write dictionary to disk as json file."""
    if not os.path.exists(os.path.dirname(fname)):
        os.makedirs(os.path.dirname(fname))
    with open(fname, "w") as fp:
        json.dump(dictionary, fp)
    return
