from urllib.request import urlretrieve
import time


def main():
    """
    This program retrieve the latest planned services of the day from SNCF API
    """
    path_gtfs = "https://eu.ftp.opendatasoft.com/sncf/gtfs/transilien-gtfs.zip"
    today_date = time.strftime('%Y-%m-%d', time.localtime())
    path_where_download = "../../data/external/transilien-gtfs-" + today_date + ".zip"

    urlretrieve(path_gtfs, path_where_download)


if __name__ == "__main__":
    main()
