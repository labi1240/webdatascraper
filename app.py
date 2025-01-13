import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
from scraper import paginate_results
import utils
import plotly.express as px

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

            # Clean up numerical columns
            numeric_columns = ['price', 'originalListPrice', 'priceLow', 'squareFeet']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

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
    with st.expander("Filters", expanded=True):
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
            "status": st.column_config.TextColumn(
                "Status",
                help="Current status of the listing"
            ),
            "displayStatus": st.column_config.TextColumn(
                "Display Status",
                help="Display status of the listing"
            ),
            "price": st.column_config.NumberColumn(
                "Price",
                format="$%d",
                help="Current listing price"
            ),
            "originalListPrice": st.column_config.NumberColumn(
                "Original Price",
                format="$%d",
                help="Original listing price"
            ),
            "priceLow": st.column_config.NumberColumn(
                "Price Low",
                format="$%d",
                help="Lowest price for the listing"
            ),
            "streetAddress": st.column_config.TextColumn(
                "Address",
                help="Property street address"
            ),
            "city": st.column_config.TextColumn(
                "City",
                help="Property city location"
            ),
            "postalCode": st.column_config.TextColumn(
                "Postal Code",
                help="Property postal code"
            ),
            "typeName": st.column_config.TextColumn(
                "Property Type",
                help="Type of property"
            ),
            "style": st.column_config.TextColumn(
                "Style",
                help="Property style"
            ),
            "bedrooms": st.column_config.NumberColumn(
                "Beds",
                help="Number of bedrooms"
            ),
            "bathrooms": st.column_config.NumberColumn(
                "Baths",
                help="Number of bathrooms"
            ),
            "squareFeet": st.column_config.NumberColumn(
                "Square Feet",
                help="Property square footage"
            ),
            "daysOnMarket": st.column_config.NumberColumn(
                "Days on Market",
                help="Number of days the property has been listed"
            ),
            "latitude": st.column_config.NumberColumn(
                "Latitude",
                format="%.6f",
                help="Property latitude coordinates"
            ),
            "longitude": st.column_config.NumberColumn(
                "Longitude",
                format="%.6f",
                help="Property longitude coordinates"
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

    # Price Analysis
    if 'price' in df.columns:
        with st.expander("Price Analysis", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                if 'city' in df.columns:
                    st.subheader("Average Price by City")
                    city_prices = filtered_df.groupby('city')['price'].mean().round(2)
                    fig_city = px.bar(
                        x=city_prices.index,
                        y=city_prices.values,
                        title="Average Price by City",
                        labels={'x': 'City', 'y': 'Average Price ($)'}
                    )
                    st.plotly_chart(fig_city, use_container_width=True)

            with col2:
                if 'typeName' in df.columns:
                    st.subheader("Average Price by Property Type")
                    type_prices = filtered_df.groupby('typeName')['price'].mean().round(2)
                    fig_type = px.bar(
                        x=type_prices.index,
                        y=type_prices.values,
                        title="Average Price by Property Type",
                        labels={'x': 'Property Type', 'y': 'Average Price ($)'}
                    )
                    st.plotly_chart(fig_type, use_container_width=True)

    # Property Distribution
    with st.expander("Property Distribution", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            if 'typeName' in df.columns:
                st.subheader("Property Types Distribution")
                type_counts = filtered_df['typeName'].value_counts()
                fig_type_pie = px.pie(
                    values=type_counts.values,
                    names=type_counts.index,
                    title="Property Types Distribution"
                )
                st.plotly_chart(fig_type_pie, use_container_width=True)

        with col2:
            if 'style' in df.columns:
                st.subheader("Property Styles Distribution")
                style_counts = filtered_df['style'].value_counts()
                fig_style_pie = px.pie(
                    values=style_counts.values,
                    names=style_counts.index,
                    title="Property Styles Distribution"
                )
                st.plotly_chart(fig_style_pie, use_container_width=True)

elif not st.session_state.scraping_complete:
    st.info("Select a date range and click 'Start Scraping' to begin data collection.")