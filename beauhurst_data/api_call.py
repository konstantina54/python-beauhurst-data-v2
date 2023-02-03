import time
from datetime import date

# from prefect import flow, task, get_run_logger
from utils.api_access import authenticate, call_api
from utils.json_processor import save_json
from generate_csv import json_to_csv, flatten_collection


# @flow
def extract_collection(collection_id: str = "8y3mrn"):
    """
    Start going through the collection we have created in beauhurst. After collecting beauhurst_id there is a second api
    call collecting information for the company. Looping through the data it divides in paths for handling. To avoid throttling when we exceed limits there is a
    timer delaying the calls, could be optimised. Script stops running once there are no
    more results returned from first collection.

    :type collection_id: the test collection is 'j8gcsi' (tech sector >35k results)
                        yppin7' (companies without company number in hubspot)
    """
    # logger = get_run_logger()
    # i = 100
    # while i > 0:
    #     logger.info(f"Countdown: {i}")
    #     time.sleep(0.1)


    page_counter = 0
    count = 1
    while count > 0:
        results, meta = get_ids(page_counter, collection_id)
        page_counter += meta["limit"]
        count = meta["count"]

        get_collection(page_counter, results)


# @flow
def get_collection(page_counter, results):
    for company in results:
        beauhurst_id = company["id"]
        results = get_company_info(beauhurst_id)
        save_json(flatten_collection(results), f"json_data_{date.today()}", lines=True)
        json_to_csv(flatten_collection(results), f"csv_data_{date.today()}")
    print("done count: ", page_counter)
    time.sleep(15)


# @task
def get_ids(page_counter: int, collection_id: str):
    headers = authenticate()
    endpoint = f"/companies/search"
    params = {
        "collection_id": collection_id,
        "limit": 15,
        "offset": str(page_counter),
    }

    r = call_api(endpoint, headers, params)
    return r["results"], r["meta"]


# @task
def get_company_info(company_id: str):
    headers = authenticate()
    all_includes = (
        "?includes=name&includes=registered_name&includes=registration_date&includes=other_trading_names"
        "&includes=companies_house_id&includes=employee_count_range&includes=last_modified_date&includes"
        "=beauhurst_url&includes=website&includes=tracked_status&includes=started_tracking_at&includes"
        "=company_status&includes=is_sme&includes=sectors&includes=top_level_sector_groups&includes"
        "=buzzwords&includes=latest_stage_of_evolution&includes=stage_of_evolution_transitions&includes"
        "=description&includes=tracking_reasons&includes=n_fundraisings&includes=total_amount_fundraisings"
        "&includes=n_grants&includes=total_amount_grants&includes=latest_valuation&includes=country"
        "&includes=lep&includes=region&includes=postcode&includes=address&includes=emails&includes"
        "=telephone&includes=year_end_date&includes=turnover&includes=ebitda&includes=total_assets"
        "&includes=number_of_employees&includes=twitter_handle&includes=instagram_handle&includes"
        "=pinterest_handle&includes=facebook_url&includes=googleplay_url&includes=itunes_url&includes"
        "=linkedin_url "
    )
    endpoint = f"/companies/{company_id}" + all_includes

    return call_api(endpoint, headers)


if __name__ == "__main__":
    extract_collection()

