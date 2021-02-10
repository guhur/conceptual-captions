# -*- coding: utf-8 -*-
import math
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
    correspondance: Path = Path("correspondance")
    image_folder: Path = Path("images")
    num_rows: int = 3318333
    chunksize: int = 128


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


def image_downloader(correspondance: Path):
    """
    Input:
    param: img_url  str (Image url)
    Tries to download the image url and use name provided in headers. Else it randomly picks a name
    """
    with open(correspondance, "r") as f:
        num_rows = sum(1 for _ in f)

    with open(correspondance, newline="") as f:
        reader = csv.DictReader(
            f, delimiter="\t", fieldnames=("caption", "url", "location")
        )

        for row in tqdm(reader, total=num_rows):
            try:
                download_url(row["url"], Path(row["location"]))
            except:  # OSError:
                continue


def run_downloader(args: Arguments):
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
                args.correspondance.iterdir(),
                chunksize=1,
            )
        )


def make_correspondance(args: Arguments):
    """
    It adds to the CSV file to location of the files
    """
    args.correspondance.mkdir(parents=True)

    with open(args.csv, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t", fieldnames=("caption", "url"))
        per_split = math.ceil(args.num_rows / args.num_proc)

        for proc_id in trange(args.num_proc):
            correspondance_file = (
                args.correspondance / f"{args.csv.stem}.part-{proc_id}.tsv"
            )
            with open(correspondance_file, "w") as fid:
                for row, _ in zip(reader, range(per_split)):
                    url = args.image_folder / str(proc_id) / get_path(row["url"])
                    url.parent.mkdir(exist_ok=True, parents=True)
                    fid.write("\t".join([row["caption"], row["url"], str(url)]))
                    fid.write("\n")


if __name__ == "__main__":
    args = Arguments()
    print(args.to_string(width=80))

    if not args.correspondance.is_dir():
        print("Making correspondance")
        make_correspondance(args)

    # assert len(list(args.correspondance.iterdir())) == args.num_proc

    # image_downloader(list(args.correspondance.iterdir())[0])
    run_downloader(args)
