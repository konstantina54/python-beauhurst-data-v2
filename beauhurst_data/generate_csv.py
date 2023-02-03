# from prefect import flow, task
import csv
from pathlib import Path
from utils.api_access import DATA
import os

# @flow
def flatten_collection(raw_data):
    """
    raw_data: data arriving from api call
    returns: flatten results
    """
    flat_results = {}

    def flatten(raw_data, name=''):
        if type(raw_data) is dict:
            for data in raw_data:
                flatten(raw_data[data], name + data + '_')
        elif type(raw_data) is list:
            i = 0
            for data in raw_data:
                flatten(data, name + str(i) + '_')
                i += 1
        else:
            flat_results[name[:-1]] = raw_data
    flatten(raw_data)
    return flat_results


# @task
def json_to_csv(json_file, output_path: str):
    output_path = Path(output_path)
    file_path = DATA / output_path
    # print(json_file)
    header = json_file.keys()
    rows = json_file.values()
    # print("keys are:", json_file.keys())
    # print(rows)

    #  why does is seperate every letter with comma instead of adding it as a whole value it is looping through the words
    suffix = ".csv"
    file_path = file_path.with_suffix(suffix)

    # if os.path.exists(file_path):
    #     os.remove(file_path)

    with open(file_path, 'a') as csvfile:
        task_writer = csv.writer(csvfile)
        task_writer.writerow(header)
        task_writer.writerow(rows)