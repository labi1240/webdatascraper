import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import time
from scraper import paginate_results
import utils

# Page config
st.set_page_config(
    page_title="Real Estate Data Scraper",
    page_icon="🏠",
    layout="wide"
)

# Initialize session state variables
if 'scraping_complete' not in st.session_state:
    st.session_state.scraping_complete = False
if 'data' not in st.session_state:
    st.session_state.data = None
if 'progress' not in st.session_state:
    st.session_state.progress = 0

def run_scraper():
    """Execute the scraping process with the selected date range."""
    try:
        st.session_state.data = None
        st.session_state.scraping_complete = False
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Convert dates to expected format
        all_data = paginate_results(
            start_date=st.session_state.end_date,
            end_date=st.session_state.start_date,
            progress_callback=lambda p, msg: utils.update_progress(p, msg, progress_bar, status_text)
        )
        
        if all_data:
            # Convert to DataFrame for display
            df = pd.DataFrame(all_data)
            st.session_state.data = df
            st.session_state.scraping_complete = True
            
            # Save to JSON
            with open('latest_scrape.json', 'w') as f:
                json.dump(all_data, f, indent=4)
                
        progress_bar.progress(100)
        status_text.text("Scraping completed!")
        
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

# Main layout
st.title("🏠 Real Estate Data Scraper")

# Sidebar for controls
with st.sidebar:
    st.header("Scraping Controls")
    
    # Date selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            datetime.now() - timedelta(days=7),
            key="start_date"
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            datetime.now(),
            key="end_date"
        )
    
    # Start scraping button
    if st.button("Start Scraping", type="primary"):
        run_scraper()

# Main content area
if st.session_state.scraping_complete and st.session_state.data is not None:
    df = st.session_state.data
    
    # Data overview
    st.header("Data Overview")
    st.write(f"Total listings found: {len(df)}")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        price_range = st.slider(
            "Price Range",
            float(df['price'].min()),
            float(df['price'].max()),
            (float(df['price'].min()), float(df['price'].max()))
        )
    with col2:
        cities = st.multiselect(
            "Cities",
            options=sorted(df['city'].unique()),
            default=sorted(df['city'].unique())
        )
    with col3:
        property_types = st.multiselect(
            "Property Types",
            options=sorted(df['typeName'].unique()),
            default=sorted(df['typeName'].unique())
        )
    
    # Filter the dataframe
    filtered_df = df[
        (df['price'].between(*price_range)) &
        (df['city'].isin(cities)) &
        (df['typeName'].isin(property_types))
    ]
    
    # Display filtered data
    st.dataframe(
        filtered_df,
        column_config={
            "price": st.column_config.NumberColumn(
                "Price",
                format="$%d"
            ),
            "daysOnMarket": "Days on Market",
            "streetAddress": "Address",
            "typeName": "Property Type"
        },
        hide_index=True
    )
    
    # Download buttons
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="Download JSON",
            data=filtered_df.to_json(orient='records'),
            file_name="real_estate_data.json",
            mime="application/json"
        )
    with col2:
        st.download_button(
            label="Download CSV",
            data=filtered_df.to_csv(index=False),
            file_name="real_estate_data.csv",
            mime="text/csv"
        )
    
    # Visualizations
    st.header("Data Visualization")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Average Price by City")
        city_prices = filtered_df.groupby('city')['price'].mean()
        st.bar_chart(city_prices)
        
    with col2:
        st.subheader("Property Types Distribution")
        type_counts = filtered_df['typeName'].value_counts()
        st.pie_chart(type_counts)
        
elif not st.session_state.scraping_complete:
    st.info("Select a date range and click 'Start Scraping' to begin data collection.")
