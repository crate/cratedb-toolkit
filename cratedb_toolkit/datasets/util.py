import os
import zipfile
from unittest.mock import patch


def load_dataset_kaggle(dataset: str, path: str, file_name: str = None):
    """
    Download complete dataset or individual files from Kaggle.

    To make it work, please supply Kaggle authentication information.

    Synopsis:

        export KAGGLE_USERNAME=foobar
        export KAGGLE_KEY=2b1dac2af55caaf1f34df76236fada5f
        kaggle datasets list -s weather
    """
    from kaggle import api as kaggle_api

    if file_name is None:
        with patch("os.remove"):
            kaggle_api.dataset_download_files(dataset=dataset, path=path, unzip=True)
    else:
        kaggle_api.dataset_download_file(dataset=dataset, file_name=file_name, path=path)
        outfile = os.path.join(path, file_name + ".zip")
        with zipfile.ZipFile(outfile) as z:
            z.extractall(path)
