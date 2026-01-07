"""
FMI Weather Data Visualization Dashboard
=========================================
Interactive dashboard showing:
- Map of all weather stations with latest observations
- Daily statistics and summaries
- Long-term trends for selected stations
- Data quality metrics

Author: Sherif Elashmawy
Project: FMI Weather Pipeline
Date: December 2025
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="FMI Weather Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better appearance
st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    </style>
    """, unsafe_allow_html=True)


# Configuration
PROJECT_ID = 'data-analytics-project-482302'
DATASET_ID = 'fmi_weather'
CREDENTIALS_PATH = '/home/ubuntu/config/data-analytics-project-482302-254e4fd06277.json'

# Selected stations for detailed view (4 cities with working longterm tables)
SELECTED_STATIONS = {
    100971: 'Turku',
    101846: 'Rovaniemi lentoasema',
}


@st.cache_resource
def get_bigquery_client():
    """
    Initialize BigQuery client with caching.
    Only runs once, then reuses connection.
    """
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_latest_observations(_client, selected_date):
    """
    Load latest observations from all stations.
    Refreshes every 10 minutes.
    """
    query = f"""
        WITH latest_obs AS (
          SELECT 
            fmisid,
            location,
            latitude,
            longitude,
            temperature,
            humidity,
            wind_speed,
            pressure,
            timestamp,
            ROW_NUMBER() OVER (PARTITION BY fmisid ORDER BY timestamp DESC) as rn
          FROM `{PROJECT_ID}.{DATASET_ID}.raw_observations`
          WHERE DATE(timestamp) = '{selected_date}'
        )
        SELECT * EXCEPT(rn)
        FROM latest_obs
        WHERE rn = 1
    """
    return _client.query(query).to_dataframe()


@st.cache_data(ttl=600)
def load_daily_summary(_client, date_suffix):
    """
    Load daily summary for a specific date.
    """
    try:
        query = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.daily_summary_{date_suffix}`
            ORDER BY fmisid
        """
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.warning(f"No summary data for date {date_suffix}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_quality_report(_client, date_suffix):
    """
    Load quality report for a specific date.
    """
    try:
        query = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET_ID}.quality_report_{date_suffix}`
        """
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.warning(f"No quality report for date {date_suffix}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_station_longterm(_client, fmisid, days=30):
    """
    Load long-term data for a specific station.
    """
    query = f"""
        SELECT 
            timestamp,
            temperature,
            humidity,
            wind_speed,
            pressure,
            data_completeness_score
        FROM `{PROJECT_ID}.{DATASET_ID}.station_{fmisid}_longterm`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        ORDER BY timestamp
    """
    try:
        return _client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error loading data for station {fmisid}: {e}")
        return pd.DataFrame()


def create_map_view(df):
    """
    Create interactive map of weather stations.
    """
    if df.empty:
        st.warning("No data available for map view")
        return
    
    # Handle missing wind_speed values for marker size
    df_map = df.copy()
    df_map['wind_speed_display'] = df_map['wind_speed'].fillna(0)  # Replace NaN with 0
    df_map['marker_size'] = df_map['wind_speed_display'].clip(lower=1)  # Minimum size of 1
    
    # Create color scale for temperature
    fig = px.scatter_mapbox(
        df_map,
        lat='latitude',
        lon='longitude',
        hover_name='location',
        hover_data={
            'temperature': ':.1f',
            'humidity': ':.1f',
            'wind_speed': ':.1f',
            'pressure': ':.1f',
            'latitude': False,
            'longitude': False,
            'wind_speed_display': False,  # Hide from hover
            'marker_size': False  # Hide from hover
        },
        color='temperature',
        size='marker_size',  # ✅ Use cleaned wind speed
        color_continuous_scale='RdYlBu_r',
        zoom=4,
        height=500,
        size_max=15
    )
    
    fig.update_layout(
        mapbox_style="open-street-map",
        margin={"r": 0, "t": 0, "l": 0, "b": 0}
    )
    
    return fig


def create_timeseries_plot(df, station_name):
    """
    Create time series plot for temperature and humidity.
    """
    if df.empty:
        st.warning(f"No data available for {station_name}")
        return
    
    # Create subplots (2 rows, 1 column)
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Temperature (°C)', 'Humidity (%)'),
        vertical_spacing=0.15
    )
    
    # Temperature line
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'],
            y=df['temperature'],
            mode='lines',
            name='Temperature',
            line=dict(color='red', width=2)
        ),
        row=1, col=1
    )
    
    # Humidity line
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'],
            y=df['humidity'],
            mode='lines',
            name='Humidity',
            line=dict(color='blue', width=2)
        ),
        row=2, col=1
    )
    
    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="°C", row=1, col=1)
    fig.update_yaxes(title_text="%", row=2, col=1)
    
    fig.update_layout(
        height=500,
        showlegend=False,
        title_text=f"Weather Trends - {station_name}"
    )
    
    return fig


def main():
    """
    Main Streamlit app
    """
    
    # Title and description
    st.title("🌤️ FMI Weather Data Dashboard")
    st.markdown("Real-time weather observations from Finnish Meteorological Institute")
    
    # Initialize BigQuery client
    try:
        client = get_bigquery_client()
    except Exception as e:
        st.error(f"Failed to connect to BigQuery: {e}")
        st.error("Make sure GOOGLE_APPLICATION_CREDENTIALS is set correctly")
        st.stop()
    
    # Sidebar - Settings
    st.sidebar.header("Settings")
    
    # Date selector
    selected_date = st.sidebar.date_input(
        "Select date",
        value=datetime.now().date() - timedelta(days=1),
        max_value=datetime.now().date()
    )
    date_suffix = selected_date.strftime('%Y%m%d')
    
    # View selector
    view_type = st.sidebar.selectbox(
        "Select view",
        ["Overview", "Daily Statistics", "Long-term Trends", "Data Quality"]
    )
    
    # ========================================
    # OVERVIEW PAGE
    # =======================================
    if view_type == "Overview":
        st.header(f"Daily Observations - {selected_date}")
        
        with st.spinner("Loading latest observations..."):
            latest_df = load_latest_observations(client, selected_date)
        
        if not latest_df.empty:
            # Key metrics in 4 columns
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Active Stations",
                    len(latest_df),
                    help="Number of stations with recent data"
                )
            
            with col2:
                avg_temp = latest_df['temperature'].mean()
                st.metric(
                    "Average Temperature",
                    f"{avg_temp:.1f}°C",
                    help="Average across all stations"
                )
            
            with col3:
                avg_humidity = latest_df['humidity'].mean()
                st.metric(
                    "Average Humidity",
                    f"{avg_humidity:.1f}%",
                    help="Average across all stations"
                )
            
            with col4:
                max_wind = latest_df['wind_speed'].max()
                st.metric(
                    "Max Wind Speed",
                    f"{max_wind:.1f} m/s",
                    help="Maximum wind speed observed"
                )
            
            st.markdown("---")
            
            # Map view
            st.subheader("Station Map")
            map_fig = create_map_view(latest_df)
            if map_fig:
                st.plotly_chart(map_fig, use_container_width=True)
            
            # Data table
            st.subheader("Latest Observations Table")
            st.dataframe(
                latest_df[['location', 'temperature', 'humidity', 'wind_speed', 'pressure', 'timestamp']]
                .sort_values('timestamp', ascending=False),
                use_container_width=True
            )
    
    # ========================================
    # DAILY STATISTICS PAGE
    # ========================================
    elif view_type == "Daily Statistics":
        st.header(f"Daily Statistics - {selected_date}")
        
        # Load daily summary
        with st.spinner("Loading daily statistics..."):
            summary_df = load_daily_summary(client, date_suffix)
        
        if not summary_df.empty:
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Stations Reporting",
                    len(summary_df)
                )
            
            with col2:
                total_obs = summary_df['total_observations'].sum()
                st.metric(
                    "Total Observations",
                    f"{total_obs:,}"
                )
            
            with col3:
                avg_quality = summary_df['avg_completeness_score'].mean()
                st.metric(
                    "Avg Data Quality",
                    f"{avg_quality:.2%}"
                )
            
            st.markdown("---")
            
            # Temperature statistics
            st.subheader("Temperature Statistics")
            col1, col2 = st.columns(2)
            
            with col1:
                # Hottest locations
                hottest = summary_df.nlargest(10, 'max_temperature')[['location', 'max_temperature']]
                st.write("**Hottest Locations**")
                st.dataframe(hottest, use_container_width=True)
            
            with col2:
                # Coldest locations
                coldest = summary_df.nsmallest(10, 'min_temperature')[['location', 'min_temperature']]
                st.write("**Coldest Locations**")
                st.dataframe(coldest, use_container_width=True)
            
            # Full summary table
            st.subheader("Complete Daily Summary")
            st.dataframe(summary_df, use_container_width=True)
        else:
            st.info("No processed data available for this date. Run Airflow DAG to process data.")
    
    # ========================================
    # LONG-TERM TRENDS PAGE
    # ========================================
    elif view_type == "Long-term Trends":
        st.header("Long-term Weather Trends")
        
        # Station selector
        selected_station = st.selectbox(
            "Select station",
            options=list(SELECTED_STATIONS.keys()),
            format_func=lambda x: SELECTED_STATIONS[x]
        )
        
        # Time range selector
        days_range = st.slider("Days of history", min_value=7, max_value=90, value=30)
        
        # Load and plot data
        with st.spinner(f"Loading data for {SELECTED_STATIONS[selected_station]}..."):
            station_df = load_station_longterm(client, selected_station, days=days_range)
        
        if not station_df.empty:
            # Time series plot
            fig = create_timeseries_plot(station_df, SELECTED_STATIONS[selected_station])
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            
            # Additional metrics
            st.subheader("Statistics")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Avg Temperature",
                    f"{station_df['temperature'].mean():.1f}°C"
                )
            
            with col2:
                temp_range = f"{station_df['temperature'].min():.1f}°C to {station_df['temperature'].max():.1f}°C"
                st.metric(
                    "Temperature Range",
                    temp_range
                )
            
            with col3:
                st.metric(
                    "Avg Humidity",
                    f"{station_df['humidity'].mean():.1f}%"
                )
            
            with col4:
                st.metric(
                    "Data Points",
                    f"{len(station_df):,}"
                )
        else:
            st.info("No long-term data available. Run Airflow DAG to create long-term tables.")
    
    # ========================================
    # DATA QUALITY PAGE
    # ========================================
    elif view_type == "Data Quality":
        st.header(f"Data Quality Report - {selected_date}")
        
        # Load quality report
        with st.spinner("Loading quality report..."):
            quality_df = load_quality_report(client, date_suffix)
        
        if not quality_df.empty:
            report = quality_df.iloc[0]
            
            # Overall rating
            rating = report['overall_quality_rating']
            rating_color = {
                'EXCELLENT': '🟢',
                'GOOD': '🟡',
                'FAIR': '🟠',
                'POOR': '🔴'
            }.get(rating, '⚪')
            
            st.markdown(f"## Overall Quality: {rating_color} {rating}")
            st.markdown("---")
            
            # Metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Active Stations", int(report['active_stations']))
                st.metric("Total Observations", f"{int(report['total_observations']):,}")
            
            with col2:
                st.metric("Temperature Valid", f"{report['temperature_valid_pct']:.1f}%")
                st.metric("Humidity Valid", f"{report['humidity_valid_pct']:.1f}%")
            
            with col3:
                st.metric("Wind Speed Valid", f"{report['wind_speed_valid_pct']:.1f}%")
                st.metric("Pressure Valid", f"{report['pressure_valid_pct']:.1f}%")
            
            st.markdown("---")
            
            # Missing data chart
            st.subheader("Missing Data")
            missing_data = pd.DataFrame({
                'Parameter': ['Temperature', 'Humidity', 'Wind Speed', 'Pressure'],
                'Missing Count': [
                    int(report['missing_temperature']),
                    int(report['missing_humidity']),
                    int(report['missing_wind_speed']),
                    int(report['missing_pressure'])
                ]
            })
            st.bar_chart(missing_data.set_index('Parameter'))
            
            # Outliers chart
            st.subheader("Outliers Detected")
            outlier_data = pd.DataFrame({
                'Parameter': ['Temperature', 'Humidity', 'Wind Speed', 'Pressure'],
                'Outlier Count': [
                    int(report['outlier_temperature']),
                    int(report['outlier_humidity']),
                    int(report['outlier_wind_speed']),
                    int(report['outlier_pressure'])
                ]
            })
            st.bar_chart(outlier_data.set_index('Parameter'))
        else:
            st.info("No quality report available for this date. Run Airflow DAG to generate reports.")
    
    # Footer
    st.markdown("---")
    st.markdown("*Data source: Finnish Meteorological Institute (FMI) | Last updated: " + 
                datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "*")


if __name__ == "__main__":
    main()
