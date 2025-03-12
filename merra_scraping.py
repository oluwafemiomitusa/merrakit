"""
MERRA-2 Data Downloader and Processor

This script downloads and processes meteorological data from NASA's MERRA-2 database.
It supports parallel downloading and processing of multiple variables for multiple locations.

Example usage:
    # Set your credentials
    username = 'your_username'
    password = 'your_password'
    
    # Use default West African locations or specify your own
    my_locs = [
        ('london', '51.5074', '-0.1278'),
        ('paris', '48.8566', '2.3522')
    ]
    
    # Run with custom settings
    years = list(range(2020, 2024))  # Last 4 years
    BASE_DATA_DIR = '/path/to/data'
    NUMBER_OF_CONNECTIONS = 5
    NUMBER_OF_PROCESSES = 2
"""

import os
import re
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from calendar import monthrange
from opendap_download.multi_processing_download import DownloadManager
import ast  # for safely evaluating string lambda functions

####### DEFAULT CONFIGURATIONS #########

# Default locations - West African meteorological stations
DEFAULT_LOCATIONS = [
    ('dakar', '14.74', '-17.49'),
    ('bamako', '12.63', '-8.03'),
    ('niamey', '13.48', '2.17'),
    ('ouagadougou', '12.35', '-1.52'),
    ('abuja', '9.06', '7.49')
]

####### USER CONFIGURATIONS - MODIFY THESE #########

# Credentials (Required)
username = ''  # Your MERRA-2 account username
password = ''  # Your MERRA-2 account password

# Time range
years = list(range(2008, 2025))  # Default: 2008-2024

# Locations (use DEFAULT_LOCATIONS or specify your own)
locs = DEFAULT_LOCATIONS

# File paths
BASE_DATA_DIR = '/Volumes/One Touch/Cloud'  # Base directory for all data
VARIABLES_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'merra2_variables.csv')

# Parallel processing settings
NUMBER_OF_CONNECTIONS = 10  # Number of parallel downloads
NUMBER_OF_PROCESSES = 3    # Number of parallel processing workers

####### HELPER FUNCTIONS #########
def parse_variables_table(config_file=VARIABLES_CONFIG_FILE):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    variables_path = os.path.join(script_dir, 'merra2_variables.csv')
    df = pd.read_csv(variables_path)
    
    variables = []
    for _, row in df.iterrows():
        variables.append({ 
            'field_name': row['Variable Used in Study'],
            'field_id': row['Field ID'],
            'original_name': row['Field Name'],
            'database_name': row['Database Name'],
            'database_id': row['Database ID'],
            'conversion_function': eval(row['Conversion Function']),
            'aggregator': row['Aggregator']
        })
    return variables

# Load variables configuration
variables_config = parse_variables_table()

locs = DEFAULT_LOCATIONS # List of locations for which data will be downloaded. Each location is a three-tuple, consisting of name (string), latitude, and longitude floats)

#%%
####### CONSTANTS - DO NOT CHANGE BELOW THIS LINE #######
lat_coords = np.arange(0, 361, dtype=int)
lon_coords = np.arange(0, 576, dtype=int)
NUMBER_OF_CONNECTIONS = 5

# %%
####### HELPER FUNCTIONS #########
def extract_date(data_set):
    """
    Extracts the date from the filename before merging the datasets. 
    """ 
    if 'HDF5_GLOBAL.Filename' in data_set.attrs:
        f_name = data_set.attrs['HDF5_GLOBAL.Filename']
    elif 'Filename' in data_set.attrs:
        f_name = data_set.attrs['Filename']
    else: 
        raise AttributeError('The attribute name has changed again!')
    # find a match between "." and ".nc4" that does not have "." .
    exp = r'(?<=\.)[^\.]*(?=\.nc4)'
    res = re.search(exp, f_name).group(0)
    # Extract the date. 
    y, m, d = res[0:4], res[4:6], res[6:8]
    date_str = ('%s-%s-%s' % (y, m, d))
    data_set = data_set.assign(date=date_str)
    return data_set

# Translate lat/lon into coordinates that MERRA-2 understands
def translate_lat_to_geos5_native(latitude):
    """
    The source for this formula is in the MERRA2 
    Variable Details - File specifications for GEOS pdf file.
    The Grid in the documentation has points from 1 to 361 and 1 to 576.
    The MERRA-2 Portal uses 0 to 360 and 0 to 575.
    latitude: float Needs +/- instead of N/S
    """
    return ((latitude + 90) / 0.5)

def translate_lon_to_geos5_native(longitude):
    """See function above"""
    return ((longitude + 180) / 0.625)

def find_closest_coordinate(calc_coord, coord_array):
    """
    Since the resolution of the grid is 0.5 x 0.625, the 'real world'
    coordinates will not be matched 100% correctly. This function matches 
    the coordinates as close as possible. 
    """
    # np.argmin() finds the smallest value in an array and returns its
    # index. np.abs() returns the absolute value of each item of an array.
    # To summarize, the function finds the difference closest to 0 and returns 
    # its index. 
    index = np.abs(coord_array-calc_coord).argmin()
    return coord_array[index]

def translate_year_to_file_number(year):
    """
    The file names consist of a number and a meta data string. 
    The number changes over the years. 1980 until 1991 it is 100, 
    1992 until 2000 it is 200, 2001 until 2010 it is  300 
    and from 2011 until now it is 400.
    """
    file_number = ''
    
    if year >= 1980 and year < 1992:
        file_number = '100'
    elif year >= 1992 and year < 2001:
        file_number = '200'
    elif year >= 2001 and year < 2011:
        file_number = '300'
    elif year >= 2011:
        file_number = '400'
    else:
        raise Exception('The specified year is out of range.')
    return file_number

def generate_url_params(parameter, time_para, lat_para, lon_para):
    """Creates a string containing all the parameters in query form"""
    parameter = map(lambda x: x + time_para, parameter)
    parameter = map(lambda x: x + lat_para, parameter)
    parameter = map(lambda x: x + lon_para, parameter)
    return ','.join(parameter)
    
def generate_download_links(download_years, base_url, dataset_name, url_params):
    """
    Generates the links for the download. 
    download_years: The years you want to download as array. 
    dataset_name: The name of the data set. For example tavg1_2d_slv_Nx
    """
    urls = []
    for y in download_years: 
        y_str = str(y)
        file_num = translate_year_to_file_number(y)
        for m in range(1,13):
            m_str = str(m).zfill(2)
            _, nr_of_days = monthrange(y, m)
            for d in range(1,nr_of_days+1):
                d_str = str(d).zfill(2)
                # Create the file name string
                file_name = 'MERRA2_{num}.{name}.{y}{m}{d}.nc4'.format(
                    num=file_num, name=dataset_name, 
                    y=y_str, m=m_str, d=d_str)
                # Create the query
                query = '{base}{y}/{m}/{name}.nc4?{params}'.format(
                    base=base_url, y=y_str, m=m_str, 
                    name=file_name, params=url_params)
                urls.append(query)
    return urls

def get_expected_filenames(years, database_id):
    """Generate list of expected filenames for the given years"""
    expected_files = []
    for year in years:
        file_num = translate_year_to_file_number(year)
        for month in range(1, 13):
            _, days = monthrange(year, month)
            for day in range(1, days + 1):
                filename = f'MERRA2_{file_num}.{database_id}.{year}{month:02d}{day:02d}.nc4'
                expected_files.append(filename)
    return set(expected_files)

def check_download_completeness(field_name, loc, database_id, years):
    """Check if all expected files are downloaded and valid"""
    dir_path = os.path.join(BASE_DATA_DIR, field_name, loc)
    if not os.path.exists(dir_path):
        return False, []
        
    expected_files = get_expected_filenames(years, database_id)
    existing_files = set(f for f in os.listdir(dir_path) if f.endswith('.nc4'))
    missing_files = expected_files - existing_files
    
    # Also check for empty/corrupted files
    corrupted_files = []
    for file in existing_files:
        file_path = os.path.join(dir_path, file)
        if os.path.getsize(file_path) == 0:
            corrupted_files.append(file)
            
    return len(missing_files) == 0 and len(corrupted_files) == 0, list(missing_files) + corrupted_files

def check_if_data_exists(field_name, loc):
    """Check if downloaded data exists for this field and location"""
    raw_dir = os.path.join(BASE_DATA_DIR, field_name, loc)
    return os.path.exists(raw_dir)

print('DOWNLOADING AND PROCESSING MULTIPLE VARIABLES')
print('Total variables:', len(variables_config))
print(f'Processing years {years[0]} to {years[-1]}')
print('Estimated time:', len(years) * len(variables_config) * len(locs) * 6, 'minutes')
print('=====================')

for var_config in variables_config:
    field_id = var_config['field_id']
    field_name = var_config['field_name']
    database_name = var_config['database_name']
    database_id = var_config['database_id']
    conversion_function = var_config['conversion_function']
    aggregator = var_config['aggregator']
    
    print(f'\nProcessing {field_name} ({field_id})')
    print('=====================')
    
    database_url = f'https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/{database_name}.5.12.4/'
    
    for loc, lat, lon in locs:
        # Check download completeness for this field and location
        is_complete, missing_files = check_download_completeness(field_name, loc, database_id, years)
        
        if is_complete:
            print(f'All data files exist and are valid for {field_name} at {loc}, skipping...')
            continue
        elif check_if_data_exists(field_name, loc):
            print(f'Incomplete data for {field_name} at {loc}')
            print(f'Missing or corrupted files: {len(missing_files)}')
            
        print(f'Downloading {field_name} data for {loc}')
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.join('/Volumes/One Touch/Cloud', field_name, loc), exist_ok=True)
        
        # Check if raw files already exist
        dir_path = os.path.join('/Volumes/One Touch/Cloud', field_name, loc)
        existing_files = set(f.replace('.nc4', '') for f in os.listdir(dir_path) 
                           if f.endswith('.nc4'))
        
        # Generate URLs only for missing files
        lat_coord = translate_lat_to_geos5_native(float(lat))
        lon_coord = translate_lon_to_geos5_native(float(lon))
        lat_closest = find_closest_coordinate(lat_coord, lat_coords)
        lon_closest = find_closest_coordinate(lon_coord, lon_coords)
        requested_lat = f'[{lat_closest}:1:{lat_closest}]'
        requested_lon = f'[{lon_closest}:1:{lon_closest}]'
        parameter = generate_url_params([field_id], '[0:1:23]', requested_lat, requested_lon)
        all_urls = generate_download_links(years, database_url, database_id, parameter)
        
        # Filter URLs to only download missing files
        urls_to_download = [url for url in all_urls 
                          if url.split('/')[-1].split('.nc4')[0] not in existing_files]
        
        if not urls_to_download:
            print(f'All raw files already exist for {field_name} at {loc}, processing data...')
        else:
            print(f'Downloading {len(urls_to_download)} missing files in parallel...')
            
            # Split URLs into chunks for parallel downloading
            chunk_size = max(1, len(urls_to_download) // NUMBER_OF_PROCESSES)
            url_chunks = [urls_to_download[i:i + chunk_size] for i in range(0, len(urls_to_download), chunk_size)]
            
            # Create a download manager for each chunk
            download_managers = []
            for chunk in url_chunks:
                dm = DownloadManager()
                dm.set_username_and_password(username, password)
                dm.download_path = os.path.join(BASE_DATA_DIR, field_name, loc)
                dm.download_urls = chunk
                dm.start_download(NUMBER_OF_CONNECTIONS)
                download_managers.append(dm)
            
            # Process downloads sequentially since start_download is synchronous
            for i, dm in enumerate(download_managers, 1):
                print(f'Processing chunk {i} of {len(download_managers)}...')
                dm.start_download(NUMBER_OF_CONNECTIONS)

        # Process the downloaded data
        print(f'Cleaning and merging {field_name} data for {loc}')
        dfs = []
        valid_files = []
        
        # First, check which files are actually downloaded and valid
        dir_path = os.path.join('/Volumes/One Touch/Cloud', field_name, loc)
        for file in os.listdir(dir_path):
            if '.nc4' in file:
                file_path = os.path.join(dir_path, file)
                if os.path.getsize(file_path) > 0:  # Check if file is not empty
                    valid_files.append(file_path)
                else:
                    # Remove empty or corrupted files
                    os.remove(file_path)
                    print(f'Removed empty/corrupted file: {file}')
        
        if not valid_files:
            print(f'No valid data files found for {field_name} at {loc}')
            continue
            
        # Now process only the valid files
        try:
            all_data = []
            for file_path in valid_files:
                try:
                    # First try netcdf4 engine
                    with xr.open_dataset(file_path, engine='netcdf4') as dataset:
                        dataset = extract_date(dataset)
                        time_values = dataset['time'].values
                        date_str = dataset.date.values
                        base_date = pd.to_datetime(date_str)
                        
                        datetimes = [base_date + pd.Timedelta(hours=int(t)) for t in time_values]
                        data_values = dataset[field_id].squeeze().values
                        
                        df = pd.DataFrame({
                            'datetime': datetimes,
                            field_name: data_values
                        }).set_index('datetime')
                        
                        all_data.append(df)
                        
                except (ValueError, OSError) as e:
                    # Fallback to h5netcdf if netcdf4 fails
                    try:
                        with xr.open_dataset(file_path, engine='h5netcdf') as dataset:
                            dataset = extract_date(dataset)
                            time_values = dataset['time'].values
                            date_str = dataset.date.values
                            base_date = pd.to_datetime(date_str)
                            
                            datetimes = [base_date + pd.Timedelta(hours=int(t)) for t in time_values]
                            data_values = dataset[field_id].squeeze().values
                            
                            df = pd.DataFrame({
                                'datetime': datetimes,
                                field_name: data_values
                            }).set_index('datetime')
                            
                            all_data.append(df)
                            
                    except Exception as fallback_e:
                        print(f'Failed to read {file_path} with both engines: {fallback_e}')
                        continue

            if not all_data:
                print(f'No data found in files for {field_name} at {loc}')
                continue
                
            # Combine all the data
            df = pd.concat(all_data).sort_index()
            
            # Reset index to handle 'datetime' as a column
            df = df.reset_index()
            
            # Apply conversion function
            df[field_name] = df[field_name].apply(conversion_function)
            
            # Extract date and hour from datetime
            df['date'] = df['datetime'].dt.date
            df['hour'] = df['datetime'].dt.time
            
            # Save processed data
            output_dir = os.path.join(BASE_DATA_DIR, field_name, loc, 'processed')
            os.makedirs(output_dir, exist_ok=True)
            
            # Save hourly data
            df[['date', 'hour', field_name]].to_csv(
                os.path.join(output_dir, f'{field_name}_{loc}_hourly.csv'), 
                index=False
            )
            
            # Create daily aggregations
            df_daily = df.groupby('date', as_index=False).agg({field_name: aggregator})
            df_daily['date'] = pd.to_datetime(df_daily['date'])
            df_daily.to_csv(
                os.path.join(output_dir, f'{field_name}_{loc}_daily.csv'), 
                index=False
            )
            
            # Create weekly aggregations
            df_weekly = df_daily.copy()
            df_weekly['Year'] = df_daily['date'].dt.isocalendar().year
            df_weekly['Week'] = df_daily['date'].dt.isocalendar().week
            df_weekly = df_weekly.groupby(['Year', 'Week'], as_index=False).agg({field_name: aggregator})
            df_weekly.to_csv(
                os.path.join(output_dir, f'{field_name}_{loc}_weekly.csv'), 
                index=False
            )
            
            print(f'Successfully processed data for {field_name} at {loc}')
            
        except Exception as e:
            print(f'Error processing files for {field_name} at {loc}: {str(e)}')
            continue

print('FINISHED')
