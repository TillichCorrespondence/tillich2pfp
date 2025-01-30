import os
import owncloud


def upload_files_to_owncloud(file_list: list, user: str, pw: str, folder="pfp-data"):
    """
    Uploads a list of files to an OwnCloud server.
    Args:
        file_list (list): List of file paths to be uploaded.
        user (str): Username for OwnCloud login.
        pw (str): Password for OwnCloud login.
        folder (str, optional): Destination folder on the OwnCloud server. Defaults to "pfp-data".
    Returns:
        result: The result of the last file upload operation.
    """

    collection = folder
    oc = owncloud.Client("https://oeawcloud.oeaw.ac.at")
    oc.login(user, pw)

    try:
        oc.mkdir(collection)
    except:  # noqa: E722
        pass

    files = file_list
    for x in files:
        _, tail = os.path.split(x)
        owncloud_name = f"{collection}/{tail}"
        print(f"uploading {tail} to {owncloud_name}")
        result = oc.put_file(owncloud_name, x)

    return result
