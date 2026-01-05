"""
FMI Weather Client
Fetches real-time weather observations from Finnish Meteorological Institute
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FMIClient:
    """Client for fetching weather data from FMI API"""
    
    def __init__(self):
        self.base_url = "https://opendata.fmi.fi/wfs"
        self.stored_query_id = "fmi::observations::weather::multipointcoverage"
        
    def get_latest_observations(self, fmisid: str, parameters: List[str] = None) -> Optional[Dict]:
        """
        Fetch latest weather observations for a station
        
        Args:
            fmisid: FMI station ID
            parameters: List of parameters to fetch (temp, humidity, etc.)
            
        Returns:
            Dictionary with observation data or None if error
        """
        if parameters is None:
            parameters = [
                "temperature",
                "humidity", 
                "windspeedms",
                "winddirection",
                "pressure",
                "precipitation1h",
                "totalcloudcover",
                "visibility"
            ]
        
        # Time range: last 2 hours to now
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=2)
        
        params = {
            "request": "getFeature",
            "storedquery_id": self.stored_query_id,
            "fmisid": fmisid,
            "parameters": ",".join(parameters),
            "starttime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endtime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        try:
            logger.info(f"Fetching data for station {fmisid}")
            
            response = requests.get(self.base_url, params=params, timeout=30)
            logger.info(f"Response status: {response.status_code}")
            
            response.raise_for_status()
            
            # Parse XML response
            data = self._parse_multipointcoverage(response.text, fmisid)
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for station {fmisid}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for station {fmisid}: {e}", exc_info=True)
            return None
    
    def _parse_multipointcoverage(self, xml_data: str, fmisid: str) -> Optional[Dict]:
        """Parse FMI's multipointcoverage XML response"""
        
        try:
            root = ET.fromstring(xml_data)
            
            # Define namespaces
            ns = {
                'wfs': 'http://www.opengis.net/wfs/2.0',
                'gml': 'http://www.opengis.net/gml/3.2',
                'gmlcov': 'http://www.opengis.net/gmlcov/1.0',
                'swe': 'http://www.opengis.net/swe/2.0',
                'om': 'http://www.opengis.net/om/2.0'
            }
            
            # Find positions - look for gmlcov:positions (not gml:positions)
            positions = root.find('.//gmlcov:positions', ns)
            if positions is None:
                logger.error("Could not find gmlcov:positions in XML")
                return None
            
            # Find values
            values = root.find('.//gml:doubleOrNilReasonTupleList', ns)
            if values is None:
                logger.error("Could not find value data in XML")
                return None
            
            # Parse positions (each line has: lat lon timestamp)
            position_data = positions.text.strip().split('\n')
            latest_position = position_data[-1].split()
            
            # Parse values (each line has: temp humidity wind_speed wind_dir pressure precip cloud_cover visibility)
            value_data = values.text.strip().split('\n')
            latest_values = value_data[-1].split()
            
            logger.info(f"Position: {latest_position}")
            logger.info(f"Values: {latest_values}")
            
            # Build result dictionary
            result = {
                'fmisid': fmisid,
                'latitude': float(latest_position[0]),
                'longitude': float(latest_position[1]),
                'timestamp': datetime.fromtimestamp(int(latest_position[2])).isoformat(),
                'temperature': float(latest_values[0]) if latest_values[0] != 'NaN' else None,
                'humidity': float(latest_values[1]) if len(latest_values) > 1 and latest_values[1] != 'NaN' else None,
                'wind_speed': float(latest_values[2]) if len(latest_values) > 2 and latest_values[2] != 'NaN' else None,
                'wind_direction': float(latest_values[3]) if len(latest_values) > 3 and latest_values[3] != 'NaN' else None,
                'pressure': float(latest_values[4]) if len(latest_values) > 4 and latest_values[4] != 'NaN' else None,
                'precipitation': float(latest_values[5]) if len(latest_values) > 5 and latest_values[5] != 'NaN' else None,
                'cloud_cover': float(latest_values[6]) if len(latest_values) > 6 and latest_values[6] != 'NaN' else None,
                'visibility': float(latest_values[7]) if len(latest_values) > 7 and latest_values[7] != 'NaN' else None,
            }
            
            logger.info(f"Successfully parsed data for station {fmisid}")
            return result
            
        except Exception as e:
            logger.error(f"Error parsing XML: {e}", exc_info=True)
            return None


def get_weather(fmisid: str) -> Optional[Dict]:
    """
    Simple function to get latest weather for a station
    
    Args:
        fmisid: FMI station ID (e.g., "100971" for Helsinki Kaisaniemi)
        
    Returns:
        Dictionary with weather data or None
    """
    client = FMIClient()
    return client.get_latest_observations(fmisid)


if __name__ == "__main__":
    test_fmisid = "100971"
    print(f"\nTesting FMI client with station {test_fmisid}...")
    print("="*60)
    
    weather = get_weather(test_fmisid)
    
    print("="*60)
    if weather:
        print("\n✅ SUCCESS - Latest observation:")
        print(f"  Station: {weather['fmisid']}")
        print(f"  Time: {weather['timestamp']}")
        print(f"  Temperature: {weather['temperature']}°C")
        print(f"  Humidity: {weather['humidity']}%")
        print(f"  Wind: {weather['wind_speed']} m/s")
        print(f"  Pressure: {weather['pressure']} hPa")
        print(f"  Cloud Cover: {weather['cloud_cover']}")
        print(f"  Visibility: {weather['visibility']} m")
    else:
        print("\n❌ FAILED to fetch weather data")