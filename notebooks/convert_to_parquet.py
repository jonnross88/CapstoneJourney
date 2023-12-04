from pathlib import Path
from collections import defaultdict
import pandas as pd
from dask import dataframe as dd
from tqdm import tqdm

SOURCE_FOLDER = Path("../data/PDQ_DSV")
DESTINATION_FOLDER = Path("../data/processed")


def get_csv_dtypes(path, sep="}", nrows=10000):
    """Reads in a csv file and returns the dtypes of the columns"""
    data = pd.read_csv(path, sep=sep, nrows=nrows)
    return data.dtypes.to_dict()


def get_csv_dtypes_for_all_files():
    """Reads in all the csv files through get_csv_dtypes in the PDQ_DSV folder and returns a defaultdict of the dtype"""
    csv_dtypes = defaultdict(str)

    for fpath in SOURCE_FOLDER.glob("*.dsv"):
        temp_dict = get_csv_dtypes(fpath)
        for k, v in temp_dict.items():
            if k not in csv_dtypes:
                csv_dtypes[k] = v

    csv_dtypes = {k: v.name for k, v in csv_dtypes.items()}
    return csv_dtypes


def convert_to_parquet():
    """Converts all the files in the PDQ_DSV folder to parquet files"""
    # create a loop to take each file in PDQ_DSV and transform it into a parquet file format
    # create folder if the destination folder does not exist
    if not DESTINATION_FOLDER.exists():
        DESTINATION_FOLDER.mkdir(parents=True)

    # get the dtypes of the csv files
    csv_dtypes = get_csv_dtypes_for_all_files()

    for file in tqdm(SOURCE_FOLDER.glob("*.dsv")):
        # read the file in a dask dataframe
        temp_ddf = dd.read_csv(file, sep="}", dtype=csv_dtypes)
        # convert the file to parquet
        temp_ddf.to_parquet(DESTINATION_FOLDER / (file.stem + ".parquet"))
        print(f"Converted {file.name} to parquet")


if __name__ == "__main__":
    convert_to_parquet()
