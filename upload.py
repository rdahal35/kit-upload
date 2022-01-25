import requests
import os
import json
import glob
import hashlib
import time
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

start_time = time.time()


API_KEY = "Here is your API key"
submit_url = "https://api.phishfeed.com/KIT/v1/submit/"
search_url = "https://api.phishfeed.com/KIT/v1/search/"


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
        f = open(zipfile, 'rb')

        try:
            with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
                wrapped_file = CallbackIOWrapper(t.update, f, "read")
                upload = requests.put(
                    presigned_url, headers=presigned_headers, data=wrapped_file)
                if upload.status_code != 200:
                    print("Upload Failed!")

        except:
            print("Upload Failed!")

        f.close()


files = glob.glob("./zipFiles/*")

for file in files:
    file_path = os.path.abspath(file)

    duplicate = check_duplicate(file_path)
    if not duplicate:
        print("The file is not a duplicate. Starting upload %s to S3...", file_path)
        submit(file_path)
    else:
        print("The file is a duplicate. It was not uploaded!")


print("--- %s seconds ---" % (time.time() - start_time))
