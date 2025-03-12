# merrakit
A high-performance Python package for parallel downloading and processing of NASA's MERRA-2 meteorological data. Built with multi-processing capabilities and robust error handling, it efficiently manages large-scale climate data downloads while providing convenient CSV-based configuration.

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
```

#### Package Configuration
At the top of `multi_processing_download.py`, configure:
1. `username` and `password`: Your MERRA-2 account credentials
2. `years`: List of years to download (default: 2008-2024)
3. `BASE_DATA_DIR`: Base directory for data storage
4. `NUMBER_OF_CONNECTIONS`: Number of parallel download connections (default: 5)
5. `NUMBER_OF_PROCESSES`: Number of parallel processing workers (default: 3)

The script includes a comprehensive list of meteorological station locations. You can modify the `locs` list to include your desired locations, where each location is specified as (name, latitude, longitude).

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
└── [variable_name]/
    └── [location]/
        ├── MERRA2_*.nc4          # Raw downloaded files
        └── processed/
            ├── [variable]_[location]_hourly.csv
            ├── [variable]_[location]_daily.csv
            └── [variable]_[location]_weekly.csv
```

The CSV files contain:
- Hourly data: date, hour, and measured value columns
- Daily data: date and aggregated value columns (using specified aggregator)
- Weekly data: year, ISO week number, and aggregated value columns

### Credits
This script is an enhanced version of the original MERRA download script created by Emily Laiken (https://github.com/emilylaiken/merradownload), which was adapted from the Open Power System Data weather data script (https://github.com/Open-Power-System-Data/weather_data/blob/master/download_merra2.ipynb).

The enhancements in this version include parallel processing, multi-variable support, improved error handling, and better file organization while maintaining the core functionality of the original script.
