# -*- coding: utf-8 -*-
import io
from pathlib import Path
from typing import Dict, List, Iterable, Union, Tuple
import hashlib
import csv
import urllib.error
import urllib.request
import shutil
from multiprocessing.pool import Pool
from multiprocessing import RLock, current_process
from tqdm.auto import tqdm, trange
import argtyped
import socket
import signal

socket.setdefaulttimeout(1)

# Intercept Ctrl-C to exit gracefully
stop = False
def signal_handler(signal_received, frame):
    global stop
    stop = True
signal.signal(signal.SIGINT, signal_handler)


class Arguments(argtyped.Arguments):
    csv: Path
    num_proc: int = 5
    num_subdir: int = 40
    correspondance: Path = Path("correspondance")
    image_folder: Path = Path("images")
    num_rows: int = 3318333

def check_type(t: str) -> Tuple[str, bool]:
    if t == "jpeg":
        t = "jpg"
    if t not in ("jpg", "png", "gif"):
        t = "unk"
    return (t, t == "unk")

def get_filename(url: str) -> Tuple[str, bool]:
    url = url.split("?")[0].split("&")[0].split(";")[0]
    stem = hashlib.sha1(str(url).encode())

    suffix, type_unknown = check_type(Path(url).suffix.strip().strip('.').lower())

    return (f"{stem.hexdigest()}.{suffix}", type_unknown)


def download_url(url: str, type_unknown: bool, dest: Union[str, Path]) -> Union[str, Path]:
    dest = Path(dest)
    if dest.is_file():
        return dest

    if type_unknown:
        for suffix in (".jpg", ".png", ".gif"):
            new_dest = dest.with_suffix(suffix)
            if new_dest.is_file():
                return new_dest

    _, headers = urllib.request.urlretrieve(url, dest)

    # Try to guess if the type is unknown
    if type_unknown:
        suffix, type_unknown = check_type(headers.get_content_subtype())

        if type_unknown and headers.get_filename() is not None:
            suffix, type_unknown = check_type(Path(headers.get_filename()).suffix.strip().strip('.').lower())

        if not type_unknown:
            new_dest = dest.with_suffix('.{}'.format(suffix))
            shutil.move(dest, new_dest)
            dest = new_dest

    return dest

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
        for row in tqdm(rows,
                        desc="#{0}: ".format(subdir_id),
                        position=current_process()._identity[0]-1,
                        leave=True):
            if stop:
                break
            try:
                filename, type_unknown = get_filename(row["url"])
                path = subdir / filename
                path = download_url(row["url"], type_unknown, path)
            except:
                path = 'N/A'
            fid.write("\t".join([row["caption"], row["url"], str(path)]))
            fid.write("\n")


def run_downloader(args: Arguments, dataset):
    """
    Inputs:
        process: (int) number of process to run
        images_url:(list) list of images url
    """
    print(f"Running {args.num_proc} process")
    with Pool(args.num_proc,
              initializer=tqdm.set_lock,
              initargs=(tqdm.get_lock(),)) as pool:
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
