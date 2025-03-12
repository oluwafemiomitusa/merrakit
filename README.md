# merrakit 🌍

A high-performance Python package for parallel downloading and processing of NASA's MERRA-2 meteorological data. Built with multi-processing capabilities and robust error handling, it efficiently manages large-scale climate data downloads while providing convenient CSV-based configuration.

[![Python](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

This package is an enhanced fork of the [original MERRA download script](https://github.com/emilylaiken/merradownload), completely redesigned for parallel processing and multi-variable support.

## What's New in This Version
- **Multi-variable Processing**: Download and process multiple meteorological variables in a single run
- **Parallel Processing**: Significantly faster downloads using multi-processing and concurrent connections
- **CSV-based Configuration**: Easy variable configuration through a CSV file instead of manual code changes
- **Improved Error Handling**: Robust error handling for downloads and file processing
- **Automatic Validation**: Checks for missing or corrupted files with automatic retries
- **Better Organization**: Structured output directory with processed data in separate folders
- **Memory Efficient**: Processes files sequentially to handle large datasets without memory issues

### Dependencies
- Python 3 - download from https://www.python.org/downloads/
- numpy (`pip install numpy`)
- pandas (`pip install pandas`)
- xarray (`pip install xarray`)
- dateutil (`pip install python-dateutil`)
- netCDF4 (`pip install netCDF4`)  # Added for better NetCDF file handling

### 1) Setting up a Data Access Account with MERRA-2
1. Register an account at https://urs.earthdata.nasa.gov/. Note your username and password.
2. Visit "Applications" --> "Authorized Apps" and click "Approve More Applications".
3. Add "NASA GESDISC DATA ARCHIVE" for approval (scroll down the list to find it).

### 2) Configuring Variables and Download Settings
The package uses a CSV file (`merra2_variables.csv`) to configure which variables to download, making it easy to manage multiple variables without code changes.

#### Variables Configuration File Format
The `merra2_variables.csv` file defines download parameters for each variable:
- `Variable Used in Study`: Your chosen name for the variable
- `Field ID`: MERRA-2 field identifier (e.g., 'LWTUP' for TOA upwelling longwave flux)
- `Field Name`: Full name of the field
- `Database Name`: MERRA-2 database name (e.g., 'M2T1NXRAD')
- `Database ID`: MERRA-2 database identifier (e.g., 'tavg1_2d_rad_Nx')
- `Conversion Function`: Lambda function for unit conversion (e.g., `lambda x: x`)
- `Aggregator`: Method for data aggregation ('mean', 'sum', 'max', or 'min')

Example CSV entry:
```csv
Variable Used in Study,Field ID,Field Name,Database Name,Database ID,Conversion Function,Aggregator
TOA upwelling longwave flux,LWTUP,Longwave Top-of-Atmosphere Upwelling Flux,M2T1NXRAD,tavg1_2d_rad_Nx,lambda x: x,mean
precipitation,PRECTOT,Total precipitation,M2T1NXFLX,tavg1_2d_flx_Nx,lambda x: x * 86400,sum  # Convert from kg/m^2/s to mm/day
temperature,T2M,2-meter air temperature,M2T1NXFLX,tavg1_2d_flx_Nx,lambda x: x - 273.15,mean  # Convert from K to °C
wind_speed,U2M,2-meter eastward wind,M2T1NXFLX,tavg1_2d_flx_Nx,lambda x: x,mean
```

### Quick Start

1. Clone this repository:
```bash
git clone https://github.com/yourusername/merrakit.git
cd merrakit
```

2. Install dependencies:
```bash
pip install numpy pandas xarray matplotlib netCDF4 python-dateutil
```

3. Edit `merra_scraping.py` and set your MERRA-2 credentials and configuration:
```python
# Credentials
username = 'your_username'  # From earthdata.nasa.gov
password = 'your_password'

# Configuration
years = list(range(2020, 2024))  # Last 4 years
BASE_DATA_DIR = '/path/to/data'
NUMBER_OF_CONNECTIONS = 5  # Parallel downloads
NUMBER_OF_PROCESSES = 3   # Processing workers

# Use default West African locations or specify your own
locs = [
    ('london', '51.5074', '-0.1278'),
    ('paris', '48.8566', '2.3522')
]
```

4. Run the script:
```bash
python merra_scraping.py
```

### Default Settings & Customization

#### Default Locations
The script comes with a set of default meteorological stations in West Africa:
```python
DEFAULT_LOCATIONS = [
    ('dakar', '14.74', '-17.49'),
    ('bamako', '12.63', '-8.03'),
    ('niamey', '13.48', '2.17'),
    ('ouagadougou', '12.35', '-1.52'),
    ('abuja', '9.06', '7.49')
]
```

You can use these defaults or specify your own locations as shown in the Quick Start example.

#### Variable Configuration
The `merra2_variables.csv` file defines what data to download:

| Field | Description | Example |
|-------|-------------|---------|
| Variable Used in Study | Your chosen name | "temperature" |
| Field ID | MERRA-2 identifier | "T2M" |
| Field Name | Full variable name | "2-meter air temperature" |
| Database Name | MERRA-2 database | "M2T1NXFLX" |
| Database ID | Database identifier | "tavg1_2d_flx_Nx" |
| Conversion Function | Unit conversion | "lambda x: x - 273.15" |
| Aggregator | Aggregation method | "mean" |

See the example CSV entries above for common meteorological variables.

#### Processing Settings
- `NUMBER_OF_CONNECTIONS`: Controls parallel downloads (5-10 recommended)
- `NUMBER_OF_PROCESSES`: Controls parallel processing (2-4 recommended)
- `years`: List of years to download (e.g., `range(2008, 2025)`)

### 3) Running the Script
Run the script via command line:
```bash
python opendap_download/multi_processing_download.py
```

The script now provides detailed progress information:
- Total number of variables being processed
- Estimated total processing time
- Download progress for each variable and location
- Validation status of downloaded files
- Processing progress and any errors encountered

### Output Structure
Data is organized by variable and location:
```
BASE_DATA_DIR/
└── [variable_name]/             # e.g., temperature
    └── [location]/             # e.g., london
        ├── MERRA2_*.nc4       # Raw downloaded files
        └── processed/
            ├── temperature_london_hourly.csv  # Hour-by-hour data
            ├── temperature_london_daily.csv   # Daily aggregates
            └── temperature_london_weekly.csv  # Weekly aggregates
```

#### Data Formats
1. Hourly Data (`*_hourly.csv`):
```csv
date,hour,temperature
2023-01-01,00:00:00,12.5
2023-01-01,01:00:00,12.1
...
```

2. Daily Data (`*_daily.csv`):
```csv
date,temperature
2023-01-01,12.8
2023-01-02,13.2
...
```

3. Weekly Data (`*_weekly.csv`):
```csv
Year,Week,temperature
2023,1,12.9
2023,2,13.4
...
```

### Error Handling & Validation
The script includes robust error handling:
- Validates all downloads for completeness
- Automatically retries failed downloads
- Checks for and removes corrupted files
- Falls back to alternative NetCDF engines if needed
- Provides detailed progress and error reporting

The CSV files contain:
- Hourly data: date, hour, and measured value columns
- Daily data: date and aggregated value columns (using specified aggregator)
- Weekly data: year, ISO week number, and aggregated value columns

### Credits
This script is an enhanced version of the original MERRA download script created by Emily Laiken (https://github.com/emilylaiken/merradownload), which was adapted from the Open Power System Data weather data script (https://github.com/Open-Power-System-Data/weather_data/blob/master/download_merra2.ipynb).

The enhancements in this version include parallel processing, multi-variable support, improved error handling, and better file organization while maintaining the core functionality of the original script.
