# WebDataScraper

A Streamlit web application that scrapes real estate listings from a specific API, compares them with client data, and provides visualization tools for analysis.

## Features

- Scrapes real estate listings from a specific date range
- Compares scraped listings with client data to find matches
- Visualizes data with interactive charts using Plotly
- Caches data for faster subsequent runs
- Exports data to CSV and JSON formats
- Verifies the current status of previously scraped listings

## Setup Instructions

### Prerequisites

- Python 3.11 or higher
- pip (Python package installer)

### Installation

1. Clone this repository to your local machine:

   ```
   git clone https://github.com/labi1240/webdatascraper.git
   ```

2. Navigate to the project directory:

   ```
   cd webdatascraper
   ```

3. Install the required dependencies:

   ```
   pip install -r requirements.txt
   ```

### Running the Application

1. Start the Streamlit server:

   ```
   streamlit run app.py
   ```

2. Your default web browser should automatically open to `http://localhost:8501`

3. If the browser doesn't open automatically, you can manually navigate to the URL shown in the terminal output

## Usage

1. Select a date range for scraping
2. Choose whether to use cached data (if available)
3. Click "Start Scraping" to begin the data collection process
4. Once scraping is complete, you can:
   - View the scraped data in a table format
   - Upload client data to find matches
   - Visualize the data with interactive charts
   - Export the data to CSV or JSON format
   - Verify the current status of listings

## Project Structure

- `app.py`: Main Streamlit application file
- `scraper.py`: Contains the scraping logic and API interaction
- `utils.py`: Utility functions for the application
- `requirements.txt`: List of Python dependencies
- `cache/`: Directory for storing cached data

## Deployment

This application can be deployed on platforms like Render or Heroku. The included Streamlit configuration in `.streamlit/config.toml` is already set up for deployment.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
