import requests
import os
import shutil
import pathlib
import json
import glob
import hashlib
import time
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

start_time = time.time()


API_KEY = "YOUR API HERE"
submit_url = "https://b672r6l7ch.execute-api.us-east-1.amazonaws.com/submission"
search_url = "https://b672r6l7ch.execute-api.us-east-1.amazonaws.com/search"


def check_duplicate(zipfile):
    with open(zipfile, 'rb') as f:
        headers = {'x-api-key': API_KEY, 'Content-Type': 'application/json'}
        data = {
            "filter": ["kit.UUID"],
            "page_size": 1
        }

        h = hashlib.sha256()
        b = bytearray(128 * 1024)
        mv = memoryview(b)
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
            zipsha256 = h.hexdigest()
    f.close()
    data["kit.sha256"] = zipsha256

    res = requests.post(search_url, data=json.dumps(data), headers=headers)
    if res.status_code == 200 and res.json()["total_count"] is None:
        return False
    return True


def submit(zipfile):

    file_size = os.stat(zipfile).st_size

    headers = {'x-api-key': API_KEY, 'Content-Type': 'application/json'}
    data = {
        "file_name": os.path.basename(zipfile)
    }

    res = requests.post(submit_url, headers=headers, data=json.dumps(data))

    if res.status_code == 200:
        presigned_headers = {'Content-Type': 'application/binary'}
        presigned_url = res.json()['upload_url']
        with open(zipfile, 'rb') as f:
            try:
                with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
                    wrapped_file = CallbackIOWrapper(t.update, f, "read")
                    upload = requests.put(
                        presigned_url, headers=presigned_headers, data=wrapped_file)
                    if upload.status_code == 200:
                        return True
                    if upload.status_code != 200:
                        print("Upload Failed!")
            except Exception as e:
                print("Upload Failed!")
                print(e)
    return False


base_dir = pathlib.Path().resolve()
kit_files = os.path.join(base_dir, "zipFiles")
files = glob.glob(os.path.join(kit_files, "*"))
uploaded_folder = os.path.join(base_dir, "uploadedKits")

for file in files:
    file_path = os.path.abspath(file)

    duplicate = check_duplicate(file_path)
    if not duplicate:
        print("The file is not a duplicate. Starting upload %s to S3...",
              file_path)
        counter = 0
        while True:
            success = submit(file_path)
            if success:
                break
            if counter > 10:
                print("We tried to upload this file 10 times and failed!",
                      file_path)
                break
            counter = counter + 1
    else:
        print("The file is a duplicate. It was not uploaded!")

    shutil.move(file, uploaded_folder)


print("--- %s seconds ---" % (time.time() - start_time))
