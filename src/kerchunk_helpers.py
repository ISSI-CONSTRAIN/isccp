import json
import os
import zipfile

import fsspec
import kerchunk.df
import kerchunk.hdf
import tqdm
from fsspec.implementations.reference import LazyReferenceMapper
from kerchunk.combine import MultiZarrToZarr


def create_references(fs, bucket_pattern, storage_options, fname, format="json"):
    global chunk_sizes
    chunk_sizes = {}
    urls = collect_urls(fs, bucket_pattern)
    references = get_references(urls, storage_options)
    if format == "json":
        combined_ref = concat_references(references)
    elif format == "parquet":
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
            if pre_process(h5chunks.translate()["refs"]) is not None:
                references.append(h5chunks.translate())
            else:
                print(
                    f"Skipping file {u} as it has inconsistent chunks and cannot be"
                    " merged without rechunking."
                )
    return references


def reference_fs(fname):
    """Create LazyReferenceMapper."""
    fs = fsspec.filesystem("file")
    if not os.path.exists(fname):
        os.makedirs(fname)
    out = LazyReferenceMapper.create(1000, fname, fs)
    return out


def pre_process(refs):
    global chunk_sizes
    for k in list(refs):
        if ".zarray" in k:
            var = k.replace("/.zarray", "")
            current_chunks = json.loads(refs[k])["chunks"]
            if chunk_sizes.get(var, []) == []:
                chunk_sizes[var] = current_chunks
            elif chunk_sizes[var] != current_chunks:
                print(chunk_sizes[var], current_chunks)
                return None
    return refs


def concat_references(references, out=None, **kwargs):
    """Concatenate references to a single virtual zarr file."""
    mzz = MultiZarrToZarr(
        references,
        remote_protocol="s3",
        remote_options={"anon": True},
        concat_dims=["time"],
        out=out,
    )
    out_dict = mzz.translate()
    if out is not None:
        out.flush()
    return out_dict


def convert_json_to_parq(dictionary, fname):
    """Convert json reference file to parquet."""
    kerchunk.df.refs_to_dataframe(dictionary, fname)
    return


def write_reference(dictionary, fname, format="json", compression=True):
    """Write reference file to disk."""
    if not os.path.exists(os.path.dirname(fname)):
        os.makedirs(os.path.dirname(fname))
    if format == "json":
        json_str = json.dumps(dictionary)
        if compression:
            json_bytes = json_str.encode("utf-8")
            with zipfile.ZipFile(fname + ".zip", "w") as zfile:
                zfile.writestr(fname, json_bytes)
        else:
            with open(fname, "w") as fp:
                fp.write(json_str)
    elif format == "parquet":
        convert_json_to_parq(dictionary, fname)
    else:
        raise ValueError("Reference output format unknown.")
    return
