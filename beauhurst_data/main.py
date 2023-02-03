# from prefect import flow

from api_call import extract_collection
from generate_csv import flatten_collection


# @flow
def main(collection_id: str = "8y3mrn"):
    extract_collection(collection_id)
    # flatten_collection()


if __name__ == '__main__':
    main()