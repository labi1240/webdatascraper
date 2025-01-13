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
        'streetNumber', 'saleOrRent', 'squareFeetText', 'squareFeet',
        'displayAddressYN', 'listPrice', 'district', 'currency'
    ],
    '$imageSizes[0]': '150',
    '$imageSizes[1]': '600',
    '$project': 'summary',
    '$skip': '0',
    '$take': '200',
}

def create_session():
    """Create a new session with retry strategy."""
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
    return session

def fetch_results(skip, take, last_update_start, last_update_end, session):
    """Fetch a batch of listings from the API."""
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
        listings = data.get('searchResults', {}).get('data', [])

        # Process each listing to format the data
        for listing in listings:
            if 'price' in listing:
                listing['priceFormatted'] = f"${listing['price']:,.2f}"
            if 'squareFeet' in listing:
                listing['squareFeet'] = listing['squareFeet'].replace(',', '')

        return listings
    except Exception as e:
        logging.error(f"Error fetching results: {str(e)}")
        return []

def fetch_all_pages_for_date_range(formatted_date_start, formatted_date_end, progress_callback=None):
    """Fetch all pages of listings for a given date range."""
    listings = []
    skip = 0
    take = 200
    more_data = True
    session = create_session()

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

        if progress_callback:
            progress_callback(
                min(1.0, skip / (skip + take)),
                f"Fetched {len(listings)} listings for {formatted_date_start} to {formatted_date_end}"
            )

    return listings

def paginate_results(start_date, end_date, delta=timedelta(days=4), progress_callback=None):
    """Retrieve all listings by paginating through the API."""
    all_results = []
    date_ranges = []

    current_end_date = start_date
    current_start_date = start_date - delta
    iteration = 0
    max_iterations = 1000

    while current_start_date >= end_date and iteration < max_iterations:
        formatted_date_start = current_start_date.strftime('%m/%d/%Y')
        formatted_date_end = current_end_date.strftime('%m/%d/%Y')
        date_ranges.append((formatted_date_start, formatted_date_end))

        current_end_date = current_start_date - timedelta(days=1)
        current_start_date = current_end_date - delta
        iteration += 1

    max_workers = 3
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_all_pages_for_date_range, dr[0], dr[1], progress_callback): dr for dr in date_ranges}

        completed = 0
        total = len(futures)

        for future in as_completed(futures):
            try:
                listings = future.result()
                if listings:
                    all_results.extend(listings)
                completed += 1

                if progress_callback:
                    progress_callback(completed / total, f"Processing batch {completed} of {total}")

            except Exception as e:
                logging.error(f"Error in batch processing: {str(e)}")

    return all_results