import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import json
from scraper import paginate_results
import utils
import plotly.express as px

# Page config
st.set_page_config(
    page_title="Team Arora - Real Estate Data Scraper",
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
if 'use_cache' not in st.session_state:
    st.session_state.use_cache = True
if 'client_data' not in st.session_state:
    st.session_state.client_data = None
if 'address_column' not in st.session_state:
    st.session_state.address_column = None
if 'verification_complete' not in st.session_state:
    st.session_state.verification_complete = False
if 'verified_data' not in st.session_state:
    st.session_state.verified_data = None

def normalize_address(address):
    """Normalize address for comparison by removing common variations."""
    if pd.isna(address):
        return ""
    address = str(address).lower()
    # Remove common terms and extra spaces
    replacements = [
        ('avenue', 'ave'),
        ('street', 'st'),
        ('road', 'rd'),
        ('drive', 'dr'),
        ('boulevard', 'blvd'),
        ('court', 'ct'),
        (',', ''),
        ('.', ''),
        ('  ', ' ')
    ]
    for old, new in replacements:
        address = address.replace(old, new)
    return address.strip()

def find_matching_terminated_listings(terminated_df, client_df, address_column):
    """Find terminated listings that match client addresses."""
    # Normalize addresses in both dataframes
    terminated_df['normalized_address'] = terminated_df['streetAddress'].apply(normalize_address)
    client_df['normalized_address'] = client_df[address_column].apply(normalize_address)

    # Find matches
    matches = pd.merge(
        terminated_df,
        client_df,
        on='normalized_address',
        how='inner',
        suffixes=('_terminated', '_client')
    )

    return matches

def verify_listings():
    """Verify the current status of previously scraped listings."""
    try:
        if st.session_state.data is None or len(st.session_state.data) == 0:
            st.error("No listings to verify. Please run the scraper first.")
            return
            
        st.session_state.verification_complete = False
        st.session_state.verified_data = None

        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Get listing IDs from the scraped data
        listing_ids = st.session_state.data['listingID'].tolist()
        
        # Import the verification function
        from scraper import verify_listing_status
        
        # Verify the current status of listings
        verification_results = verify_listing_status(
            listing_ids=listing_ids,
            progress_callback=lambda p, msg: utils.update_progress(p, msg, progress_bar, status_text)
        )
        
        # Create a DataFrame with verification results
        import pandas as pd
        verified_data = []
        
        for listing_id, status_info in verification_results.items():
            # Find the original listing in the scraped data
            original_listing = st.session_state.data[st.session_state.data['listingID'] == listing_id].iloc[0].to_dict()
            
            # Add verification information
            original_listing.update(status_info)
            verified_data.append(original_listing)
        
        # Convert to DataFrame
        verified_df = pd.DataFrame(verified_data)
        
        # Store in session state
        st.session_state.verified_data = verified_df
        st.session_state.verification_complete = True
        
        # Complete progress
        progress_bar.progress(100)
        status_text.text("Verification completed!")
        
    except Exception as e:
        st.error(f"An error occurred during verification: {str(e)}")

def run_scraper():
    """Execute the scraping process with the selected date range."""
    try:
        st.session_state.data = None
        st.session_state.scraping_complete = False
        st.session_state.verification_complete = False
        st.session_state.verified_data = None

        progress_bar = st.progress(0)
        status_text = st.empty()

        # Convert dates to datetime objects
        start_date = datetime.combine(st.session_state.start_date, datetime.min.time())
        end_date = datetime.combine(st.session_state.end_date, datetime.min.time())

        # Execute scraping with cache control
        all_data = paginate_results(
            start_date=end_date,  # Reversed because we want newer data first
            end_date=start_date,
            progress_callback=lambda p, msg: utils.update_progress(p, msg, progress_bar, status_text),
            use_cache=st.session_state.use_cache
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
    # Add logo at the top of sidebar
    st.image("attached_assets/team-arora-logo.png", width=100)
    st.markdown("---")  # Add a separator line

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

    # Add cache control
    st.session_state.use_cache = st.checkbox(
        "Use cached data when available",
        value=True,
        help="When enabled, previously scraped data will be reused for the same date range"
    )

    # Client data upload
    st.header("Client Data")
    uploaded_file = st.file_uploader("Upload Client Data (CSV)", type=['csv'])
    if uploaded_file is not None:
        try:
            client_df = pd.read_csv(uploaded_file)
            st.session_state.client_data = client_df

            # Let user select the address column
            st.subheader("Column Mapping")
            address_columns = client_df.columns.tolist()
            st.session_state.address_column = st.selectbox(
                "Select the column containing property addresses",
                options=address_columns,
                index=address_columns.index("Address 1 - Street") if "Address 1 - Street" in address_columns else 0,
                help="Choose the column that contains the property street addresses"
            )

            st.success(f"Loaded {len(client_df)} client records")
        except Exception as e:
            st.error(f"Error loading client data: {str(e)}")

    # Start scraping button
    if st.button("Start Scraping", type="primary"):
        run_scraper()
        
    # Verification button (only enabled if scraping is complete)
    if st.session_state.scraping_complete:
        if st.button("Verify Current Status", type="secondary", help="Check if terminated listings are still terminated today"):
            verify_listings()

# Main content area
if st.session_state.verification_complete and st.session_state.verified_data is not None:
    # Display verification results
    st.header("Verification Results")
    verified_df = st.session_state.verified_data
    
    # Count listings by verification status
    still_terminated = verified_df[verified_df['still_terminated'] == True].shape[0]
    not_terminated = verified_df[verified_df['still_terminated'] == False].shape[0]
    not_found = verified_df[verified_df['found'] == False].shape[0]
    
    # Display summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Still Terminated", still_terminated)
    with col2:
        st.metric("No Longer Terminated", not_terminated)
    with col3:
        st.metric("Not Found", not_found)
    
    # Display detailed results
    st.subheader("Detailed Verification Results")
    
    # Add a status indicator column
    verified_df['Status Change'] = verified_df.apply(
        lambda row: "✅ Still Terminated" if row.get('still_terminated', False) else 
                   ("❌ Not Found" if not row.get('found', True) else "⚠️ No Longer Terminated"),
        axis=1
    )
    
    # Display the dataframe
    st.dataframe(
        verified_df[[
            'streetAddress', 'city', 'price', 'Status Change', 'status',
            'displayStatus', 'daysOnMarket', 'modified'
        ]],
        hide_index=True,
        use_container_width=True
    )
    
    # Option to continue with regular view
    if st.button("Show Original Scrape Results"):
        st.session_state.verification_complete = False
    
    # Download buttons for verification results
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="Download Verification JSON",
            data=verified_df.to_json(orient='records'),
            file_name="verification_results.json",
            mime="application/json"
        )
    with col2:
        st.download_button(
            label="Download Verification CSV",
            data=verified_df.to_csv(index=False),
            file_name="verification_results.csv",
            mime="text/csv"
        )

elif st.session_state.scraping_complete and st.session_state.data is not None:
    df = st.session_state.data

    # Show comparison with client data if available
    if st.session_state.client_data is not None and st.session_state.address_column is not None:
        st.header("Matching Terminated Listings")
        matches = find_matching_terminated_listings(df, st.session_state.client_data, st.session_state.address_column)

        if len(matches) > 0:
            st.warning(f"Found {len(matches)} terminated listings matching your client addresses!")
            st.dataframe(
                matches[[
                    'streetAddress', 'city', 'price', 'originalListPrice',
                    'daysOnMarket', 'status', 'typeName'
                ]],
                hide_index=True,
                use_container_width=True
            )
        else:
            st.success("No terminated listings found matching your client addresses.")

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
                    # Update layout for better visibility
                    fig_city.update_layout(
                        title_x=0.5,
                        yaxis_tickformat='$,.0f',
                        height=400,
                        margin=dict(t=30)
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
                    # Update layout for better visibility
                    fig_type.update_layout(
                        title_x=0.5,
                        yaxis_tickformat='$,.0f',
                        height=400,
                        margin=dict(t=30)
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
                fig_type_pie.update_layout(
                    title_x=0.5,
                    height=400,
                    margin=dict(t=30)
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
                fig_style_pie.update_layout(
                    title_x=0.5,
                    height=400,
                    margin=dict(t=30)
                )
                st.plotly_chart(fig_style_pie, use_container_width=True)

    # Neighborhoods Distribution
    if 'neighborhoods' in df.columns and 'city' in df.columns:
        st.header("Neighborhoods Distribution by City")

        # Get unique cities
        cities = sorted(filtered_df['city'].unique())

        # Create rows of two columns for each pair of cities
        for i in range(0, len(cities), 2):
            col1, col2 = st.columns(2)

            # First city in the pair
            with col1:
                city = cities[i]
                city_data = filtered_df[filtered_df['city'] == city]
                if not city_data.empty and 'neighborhoods' in city_data.columns:
                    neighborhood_counts = city_data['neighborhoods'].value_counts()
                    fig = px.pie(
                        values=neighborhood_counts.values,
                        names=neighborhood_counts.index,
                        title=f"Neighborhoods in {city}"
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Second city in the pair (if exists)
            with col2:
                if i + 1 < len(cities):
                    city = cities[i + 1]
                    city_data = filtered_df[filtered_df['city'] == city]
                    if not city_data.empty and 'neighborhoods' in city_data.columns:
                        neighborhood_counts = city_data['neighborhoods'].value_counts()
                        fig = px.pie(
                            values=neighborhood_counts.values,
                            names=neighborhood_counts.index,
                            title=f"Neighborhoods in {city}"
                        )
                        st.plotly_chart(fig, use_container_width=True)

elif not st.session_state.scraping_complete:
    st.info("Select a date range and click 'Start Scraping' to begin data collection.")