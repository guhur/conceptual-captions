# -*- coding: utf-8 -*-
import io
from pathlib import Path
from typing import Dict, List, Iterable, Union
import hashlib
import csv
import urllib.error
import urllib.request
from multiprocessing.pool import Pool
from tqdm.auto import tqdm, trange
import argtyped
import socket

socket.setdefaulttimeout(1)


class Arguments(argtyped.Arguments):
    csv: Path
    num_proc: int = 5
    num_subdir: int = 40
    correspondance: Path = Path("correspondance")
    image_folder: Path = Path("images")
    num_rows: int = 3318333


def get_path(url: str) -> str:
    url = url.split("?")[0].split("&")[0]
    stem = hashlib.sha1(str(url).encode())

    suffix = Path(url).suffix.strip()
    if suffix in (""):
        suffix = ".jpg"

    return f"{stem.hexdigest()}{suffix}"


def download_url(url: str, dest: Union[str, Path]):
    dest = Path(dest)
    if dest.is_file():
        return

    urllib.request.urlretrieve(url, dest)


def image_downloader(dataset_sub):
    """
    Input:
    param: img_url  str (Image url)
    Tries to download the image url and use name provided in headers. Else it randomly picks a name
    """
    subdir_id, rows = dataset_sub

    subdir = args.image_folder / str(subdir_id)
    subdir.mkdir(exist_ok=True, parents=True)

    correspondance_file = args.correspondance / f"{args.csv.stem}.part-{subdir_id}.tsv"

    with open(correspondance_file, "w") as fid:
        for row in tqdm(rows):
            try:
                path = subdir / get_path(row["url"])
                download_url(row["url"], path)
            except:
                pass
            fid.write("\t".join([row["caption"], row["url"], str(path)]))
            fid.write("\n")


def run_downloader(args: Arguments, dataset):
    """
    Inputs:
        process: (int) number of process to run
        images_url:(list) list of images url
    """
    print(f"Running {args.num_proc} process")
    with Pool(args.num_proc) as pool:
        list(
            pool.imap_unordered(
                image_downloader,
                dataset,
                chunksize=1,
            )
        )


def split_dataset(args: Arguments):
    """
    Split the dataset into subdirectories
    """
    num_rows_sub = args.num_rows // args.num_subdir
    num_subdir_extra = args.num_rows % args.num_subdir
    subdir_id = 0
    n = 0
    dataset = []
    dataset_sub = []

    with open(args.csv, "r") as f:
        reader = csv.DictReader(f, delimiter="\t", fieldnames=("caption", "url"))

        for row in reader:
            dataset_sub.append(row)
            n += 1
            if (n == num_rows_sub + (1 if subdir_id < num_subdir_extra else 0)):
                dataset.append((subdir_id, dataset_sub))
                n = 0
                dataset_sub = []
                subdir_id += 1

    assert(len(dataset) == args.num_subdir and len(dataset_sub) == 0)

    return dataset


if __name__ == "__main__":
    args = Arguments()
    print(args.to_string(width=80))

    print("Splitting the dataset")
    dataset = split_dataset(args)

    args.correspondance.mkdir(parents=True)
    run_downloader(args, dataset)
