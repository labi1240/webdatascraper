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

# Define cookies and headers from original code
cookies = {
    '_ga': 'GA1.3.6299276.1729021923',
    '_ga_1': 'GA1.1.6299276.1729021923',
    '_gid': 'GA1.3.1792005162.1729112353',
    '_gat': '1',
    '_gat_1': '1',
    's': 'eyJwYXNzcG9ydCI6eyJ1c2VyIjoiNTM3ZmE2ZWU1ZTU1OTlhODU1ZDJmODFiIn0sInMiOiJhYWQ5YmY5Zi03YmEzLTQxZGUtYTFjZS1jNGZkMWFjM2IyZWEiLCJub3ciOjI4ODE5OTQzLCJpIjo4MH0=',
    's.sig': 'ASpJMzN-iA35W9KNclYWvG1ZwBs',
    '_ga_G0XQP73LHV': 'GS1.1.1729193701.4.1.1729196603.21.1.1860857804',
}

headers = {
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'baggage': 'sentry-environment=production,sentry-public_key=e9f1abedeae34f04a098c5c0ebb1737a,sentry-trace_id=4d885e178ee6450ab482343ccdd4e648,sentry-sample_rate=0.1,sentry-sampled=false',
    'referer': 'https://app.realmmlp.ca/s',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Mobile Safari/537.36',
    'x-auth-userid': '537fa6ee5e5599a855d2f81b',
}

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
    '$select[0]': 'streetAddress',
    '$select[1]': 'city',
    '$select[2]': 'bedrooms',
    '$select[3]': 'bedroomsPossible',
    '$select[4]': 'bathrooms',
    '$select[5]': 'typeName',
    '$select[6]': 'style',
    '$select[7]': 'price',
    '$select[8]': 'price',
    '$select[9]': 'squareFeetText',
    '$select[10]': 'squareFeet',
    '$select[11]': 'status',
    '$select[12]': 'daysOnMarket',
    '$select[13]': 'listingID',
    '$imageSizes[0]': '150',
    '$imageSizes[1]': '600',
    '$project': 'summary',
    '$skip': '0',
    '$take': '200',
}

def create_session():
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
    session.headers.update(headers)
    session.cookies.update(cookies)
    return session

def fetch_results(skip, take, last_update_start, last_update_end, session):
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
        return listings
    except Exception as e:
        logging.error(f"Error fetching results: {str(e)}")
        return []

def fetch_all_pages_for_date_range(formatted_date_start, formatted_date_end, progress_callback=None):
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
