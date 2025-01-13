import os
import requests
import json
from datetime import datetime, timedelta
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraping.log"),
        logging.StreamHandler()
    ]
)

# Create cache directory if it doesn't exist
CACHE_DIR = "cache"
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cache_key(date_start, date_end):
    """Generate a cache key for the given date range."""
    return f"{date_start}_{date_end}"

def get_cache_file_path(cache_key):
    """Get the file path for a cache key."""
    return os.path.join(CACHE_DIR, f"{cache_key}.json")

def save_to_cache(cache_key, data):
    """Save data to cache."""
    cache_file = get_cache_file_path(cache_key)
    try:
        with open(cache_file, 'w') as f:
            json.dump(data, f)
        logging.info(f"Data saved to cache: {cache_file}")
    except Exception as e:
        logging.error(f"Error saving to cache: {e}")

def load_from_cache(cache_key):
    """Load data from cache if available."""
    cache_file = get_cache_file_path(cache_key)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            logging.info(f"Data loaded from cache: {cache_file}")
            return data
        except Exception as e:
            logging.error(f"Error loading from cache: {e}")
    return None

# Define base parameters
base_params = {
    'availability': 'U',
    'geoAnd': 'Y',
    'unavailableDate': '>=01/1/2024',
    'area': 'Peel',
    'status': 'TER',
    'district[0]': 'Brampton',
    'district[1]': 'Mississauga',
    'class': 'FREE',
    'saleOrRent': 'SALE',
    '$gid': 'treb',
    'gid': 'TREB',
    '$isMapSearch': 'true',
    '$meta[isMapSearch]': 'true',
    '$orderby': 'daysOnMarket desc',
    '$output': 'list',
    '$select': [
        'status', 'latitude', 'neighborhoods', 'originalListPrice',
        'priceLow', 'postalCode', 'price', 'class', 'modified',
        'displayStatus', 'typeName', 'longitude', 'parcelID',
        'bathrooms', 'city', 'daysOnMarket', 'bedrooms', 'listingID',
        'pricePerSquareFoot', 'streetAddress', 'style', 'streetName',
        'streetNumber', 'saleOrRent', 'squareFeetText', 'squareFeet'
    ],
    '$imageSizes[0]': '150',
    '$imageSizes[1]': '600',
    '$project': 'summary',
    '$skip': '0',
    '$take': '200'
}

def create_session():
    """
    Create a new session with retry strategy.
    This function is called by each thread to have its own session.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    # Add required headers
    session.headers.update({
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'baggage': 'sentry-environment=production,sentry-public_key=e9f1abedeae34f04a098c5c0ebb1737a,sentry-trace_id=4d885e178ee6450ab482343ccdd4e648,sentry-sample_rate=0.1,sentry-sampled=false',
        'referer': 'https://app.realmmlp.ca/s',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sentry-trace': '4d885e178ee6450ab482343ccdd4e648-ac8d50b4649cdddd-0',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36',
        'x-auth-userid': '537fa6ee5e5599a855d2f81b',
        'x-requested-with': 'XMLHttpRequest'
    })

    # Add required cookies
    session.cookies.update({
        '_ga': 'GA1.3.6299276.1729021923',
        '_ga_1': 'GA1.1.6299276.1729021923',
        '_gid': 'GA1.3.1792005162.1729112353',
        '_gat': '1',
        '_gat_1': '1',
        's': 'eyJwYXNzcG9ydCI6eyJ1c2VyIjoiNTM3ZmE2ZWU1ZTU1OTlhODU1ZDJmODFiIn0sInMiOiJhYWQ5YmY5Zi03YmEzLTQxZGUtYTFjZS1jNGZkMWFjM2IyZWEiLCJub3ciOjI4ODE5OTQzLCJpIjo4MH0=',
        's.sig': 'ASpJMzN-iA35W9KNclYWvG1ZwBs',
        '_ga_G0XQP73LHV': 'GS1.1.1729193701.4.1.1729196603.21.1.1860857804'
    })

    return session

def fetch_results(skip, take, last_update_start, last_update_end, session):
    """
    Fetch a batch of listings from the API based on the provided date range.

    :param skip: Number of records to skip (for pagination).
    :param take: Number of records to retrieve.
    :param last_update_start: Start date for filtering listings.
    :param last_update_end: End date for filtering listings.
    :param session: The requests session to use for the HTTP request.
    :return: List of listings or an empty list if none are found.
    """
    params = base_params.copy()
    params['$skip'] = str(skip)
    params['$take'] = str(take)
    params['lastUpdateDate[0]'] = f">={last_update_start}"
    params['lastUpdateDate[1]'] = f"<={last_update_end}"

    try:
        response = session.get(
            'https://app.realmmlp.ca/search',
            params=params,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        logging.debug(f"Response Data: {json.dumps(data, indent=4)}")

        # Correctly access the 'data' list within 'searchResults'
        listings = data.get('searchResults', {}).get('data', [])
        return listings
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        logging.error(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request exception: {req_err}")
    except ValueError:
        logging.error("Error decoding JSON response.")
        logging.error(f"Response content: {response.text}")
    return []

def fetch_all_pages_for_date_range(formatted_date_start, formatted_date_end):
    """
    Fetch all pages of listings for a given date range.

    :param formatted_date_start: Start date in MM/DD/YYYY format.
    :param formatted_date_end: End date in MM/DD/YYYY format.
    :return: List of listings for the date range.
    """
    listings = []
    skip = 0
    take = 200
    more_data = True
    session = create_session()  # Each thread uses its own session
    while more_data:
        batch = fetch_results(
            skip=skip,
            take=take,
            last_update_start=formatted_date_start,
            last_update_end=formatted_date_end,
            session=session
        )
        if batch:
            listings.extend(batch)
            skip += take
            if len(batch) < take:
                more_data = False
        else:
            more_data = False
    return listings

def paginate_results(start_date, end_date, delta=timedelta(days=4), progress_callback=None, use_cache=True):
    """
    Retrieve all listings by paginating through the API based on specified date ranges.
    Now with caching support.

    :param start_date: The most recent date to start fetching listings.
    :param end_date: The oldest date to stop fetching listings.
    :param delta: The time delta to decrement each iteration (default is 4 days).
    :param progress_callback: Optional callback for progress updates.
    :param use_cache: Whether to use cached data (default True).
    :return: List of all fetched listings.
    """
    # Generate cache key for the entire date range
    cache_key = get_cache_key(
        start_date.strftime('%Y%m%d'),
        end_date.strftime('%Y%m%d')
    )

    # Try to load from cache first if caching is enabled
    if use_cache:
        cached_data = load_from_cache(cache_key)
        if cached_data is not None:
            logging.info(f"Using cached data for date range {start_date} to {end_date}")
            return cached_data

    all_results = []
    date_ranges = []

    current_end_date = start_date
    current_start_date = start_date - delta
    max_iterations = 1000  # Prevent infinite loops; adjust as needed
    iteration = 0

    while current_start_date >= end_date and iteration < max_iterations:
        formatted_date_start = current_start_date.strftime('%m/%d/%Y')
        formatted_date_end = current_end_date.strftime('%m/%d/%Y')
        date_ranges.append((formatted_date_start, formatted_date_end))

        current_end_date = current_start_date - timedelta(days=1)
        current_start_date = current_end_date - delta
        iteration += 1

    logging.info(f"Total date ranges to process: {len(date_ranges)}")

    max_workers = 3  # Adjust based on your system's capability
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_all_pages_for_date_range, dr[0], dr[1]): dr
            for dr in date_ranges
        }

        for future in as_completed(futures):
            date_range = futures[future]
            try:
                listings = future.result()
                if listings:
                    all_results.extend(listings)
                    logging.info(
                        f"Fetched {len(listings)} listings for date range {date_range}. Total so far: {len(all_results)}"
                    )
                else:
                    logging.info(
                        f"No listings found for date range {date_range}.")
            except Exception as e:
                logging.error(
                    f"Error fetching data for date range {date_range}: {e}")

    logging.info(f"Total listings fetched: {len(all_results)}")

    # Save results to cache if data was fetched
    if all_results and use_cache:
        save_to_cache(cache_key, all_results)

    return all_results