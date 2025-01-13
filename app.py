import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
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

        # Convert dates to datetime objects
        start_date = datetime.combine(st.session_state.start_date, datetime.min.time())
        end_date = datetime.combine(st.session_state.end_date, datetime.min.time())

        # Execute scraping
        all_data = paginate_results(
            start_date=end_date,  # Reversed because we want newer data first
            end_date=start_date,
            progress_callback=lambda p, msg: utils.update_progress(p, msg, progress_bar, status_text)
        )

        if all_data:
            # Convert to DataFrame for display
            df = pd.DataFrame(all_data)

            # Clean up the data
            if 'price' in df.columns:
                df['price'] = pd.to_numeric(df['price'], errors='coerce')

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
        if 'price' in df.columns:
            price_range = st.slider(
                "Price Range ($)",
                float(df['price'].min()),
                float(df['price'].max()),
                (float(df['price'].min()), float(df['price'].max()))
            )
    with col2:
        if 'city' in df.columns:
            cities = st.multiselect(
                "Cities",
                options=sorted(df['city'].unique()),
                default=sorted(df['city'].unique())
            )
    with col3:
        if 'typeName' in df.columns:
            property_types = st.multiselect(
                "Property Types",
                options=sorted(df['typeName'].unique()),
                default=sorted(df['typeName'].unique())
            )

    # Filter the dataframe
    filtered_df = df.copy()
    if 'price' in df.columns:
        filtered_df = filtered_df[filtered_df['price'].between(*price_range)]
    if 'city' in df.columns and cities:
        filtered_df = filtered_df[filtered_df['city'].isin(cities)]
    if 'typeName' in df.columns and property_types:
        filtered_df = filtered_df[filtered_df['typeName'].isin(property_types)]

    # Display filtered data with proper formatting
    st.dataframe(
        filtered_df,
        column_config={
            "price": st.column_config.NumberColumn(
                "Price",
                format="$%d",
                help="Property listing price"
            ),
            "daysOnMarket": st.column_config.NumberColumn(
                "Days on Market",
                help="Number of days the property has been listed"
            ),
            "streetAddress": st.column_config.TextColumn(
                "Address",
                help="Property street address"
            ),
            "city": st.column_config.TextColumn(
                "City",
                help="Property city location"
            ),
            "typeName": st.column_config.TextColumn(
                "Property Type",
                help="Type of property"
            ),
            "bedrooms": st.column_config.NumberColumn(
                "Beds",
                help="Number of bedrooms"
            ),
            "bathrooms": st.column_config.NumberColumn(
                "Baths",
                help="Number of bathrooms"
            )
        },
        hide_index=True,
        use_container_width=True
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
        if 'city' in df.columns and 'price' in df.columns:
            st.subheader("Average Price by City")
            city_prices = filtered_df.groupby('city')['price'].mean().round(2)
            st.bar_chart(city_prices)

    with col2:
        if 'typeName' in df.columns:
            st.subheader("Property Types Distribution")
            type_counts = filtered_df['typeName'].value_counts()
            st.pie_chart(type_counts)

elif not st.session_state.scraping_complete:
    st.info("Select a date range and click 'Start Scraping' to begin data collection.")