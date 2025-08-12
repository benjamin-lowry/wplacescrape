import asyncio
import aiohttp
import pickle
from os import makedirs, listdir
from os.path import join, exists
from email.utils import parsedate_tz, mktime_tz, formatdate
from time import sleep

CONCURRENT_REQUESTS = 5
SLEEP_TIME = 1
SLEEP_TIME_429 = 5
CACHE_FILENAME = "modified_since_cache.pkl"
MAX_X = 2047
MAX_Y = 137

in_url = lambda x, y: "https://backend.wplace.live/files/s0/tiles/" + str(x) + "/" + str(y) + ".png"
out_dir = lambda x, y: join("files", "s0", "tiles", str(x), str(y))
out_path = lambda x, y, last_modified: join(out_dir(x, y), str(last_modified) + ".png")

def make_wplace_dirs():
    for x in range(MAX_X + 1):
        for y in range(MAX_Y + 1):
            makedirs(join("files", "s0", "tiles", str(x), str(y)), exist_ok=True)

async def get_tile(x, y, session, since, sem):
    retry_is_necessary = True
    async with sem:
        while retry_is_necessary:
            request_headers = {}
            if x in since and y in since[x]:
                request_headers['If-Modified-Since'] = formatdate(since[x][y], usegmt=True)
            else:
                since[x] = {}
                directory_path = out_dir(x, y)
                if exists(directory_path):
                    file_list = sorted(listdir(directory_path))
                    if len(file_list) != 0:
                        if file_list[-1] == ".DS_Store":
                            del file_list[-1]
                        if len(file_list) != 0:
                            since[x][y] = int(file_list[-1].replace(".png", ""))
                            request_headers['If-Modified-Since'] = formatdate(since[x][y], usegmt=True)
            async with session.get(in_url(x, y), headers=request_headers) as response:
                if response.status == 404:
                    print(f"The requested tile at coordinates {x}, {y} was not found on the server.")
                    retry_is_necessary = False
                    await asyncio.sleep(SLEEP_TIME)
                    return
                elif response.status == 304:
                    print(f"The tile at coordinates {x}, {y} has not been modified since the last check.")
                    retry_is_necessary = False
                    return
                elif response.status == 429:
                    print(f"A 'Too Many Requests' error (429) was received. The process will pause for {SLEEP_TIME_429} seconds before retrying.")
                    await asyncio.sleep(SLEEP_TIME_429)
                elif response.status != 200:
                    print(f"An unexpected status code of {response.status} was received for tile {x}, {y}. The process will not retry this request.")
                    retry_is_necessary = False
                    return
                else:
                    last_modified_timestamp = mktime_tz(parsedate_tz(response.headers['Last-Modified']))
                    since[x][y] = last_modified_timestamp
                    output_file_path = out_path(x, y, last_modified_timestamp)
                    with open(output_file_path, "wb") as output_file:
                        output_file.write(await response.content.read())
                    print(f"The tile at coordinates {x}, {y} has been successfully downloaded and saved.")
                    retry_is_necessary = False
                    await asyncio.sleep(SLEEP_TIME)

async def main():
    concurrency_semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    if not exists("files"):
        print("The primary 'files' directory does not exist. It will now be created along with all tile subdirectories.")
        make_wplace_dirs()
    if exists(CACHE_FILENAME):
        print("A cache file was found. The last modified timestamps will be loaded from the cache to perform conditional requests.")
        with open(CACHE_FILENAME, "rb") as cache_file:
            since = pickle.load(cache_file)
    else:
        print("No existing cache file was found. An empty cache will be initialized for this session.")
        since = {}
    async with aiohttp.ClientSession() as http_session:
        async with asyncio.TaskGroup() as task_group:
            print("A large number of asynchronous tasks are being created to download all tiles concurrently.")
            for x_coordinate in range(MAX_X + 1):
                for y_coordinate in range(MAX_Y + 1):
                    task_group.create_task(get_tile(x_coordinate, y_coordinate, http_session, since, concurrency_semaphore))
            print("All download tasks have been successfully initiated. The program will now wait for them to complete.")
    print("All tile download and verification tasks have finished. The last modified timestamps will now be saved to the cache file.")
    with open(CACHE_FILENAME, "wb") as cache_file:
        pickle.dump(since, cache_file)
    print("The cache file has been saved. The program has completed its execution.")

if __name__ == "__main__":
    print("The script is starting. Asynchronous tile downloading is beginning.")
    asyncio.run(main())
    print("The script has finished its execution.")
