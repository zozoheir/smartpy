# Installation

### In a virtual environment
This will install smart and all its dependencies (pandas, dask, fastparquet, pytorch, lightning...)
```
aws s3 cp s3://dqc-testing/dqcpy-0.1.0-py3-none-any.whl .
aws s3 cp s3://dqc-testing/requirements.txt .
pip install dqcpy-0.1.0-py3-none-any.whl
pip install -r requirements.txt
```
### In a SageMaker instance  
This creates a conda virtual environment, installs dqcpy and its dependencies, and creates a dqcpy 
kernel for notebooks. 
```
aws s3 cp s3://dqc-testing/build.sh . 
bash build.sh
```


# Release
From the git repository. This runs a poetry build and uploads the wheel file+requirements.txt to s3.
```
bash release.sh
```

# DQC Data

### Getting market data  

```python
import dqcpy.data.dqcio as io
import dqcpy.utility.os_util as os_util

# Getting data into a pandas dataframe
LOCAL_S3_DIR_PATH = r"C:\Users\otho\Documents\dqc\data\\"
dqcio = io.DQCIO(LOCAL_S3_DIR_PATH)
df = dqcio.getData(io.CONSOLIDATED_DATA, io.COINBASE, io.BTCUSD, '2021/05/14', method='pandas')
df_sample = dqcio.getData(io.CONSOLIDATED_DATA, io.COINBASE, io.BTCUSD, '2021/05/14', method='pandas',
                          sample_data=True)

# Downloading data locally 
user_home_path = os_util.getUserHomePath()
io.downloadData(io.PREDICTIONS_V0, io.COINBASE, io.BTCUSD, '2021/05/14', user_home_path)
```


# Utility

### OS Utility

```python
import dqcpy.utility.os_util as os_util

# Creating a new folder
home_path = os_util.getUserHomePath()
new_folder_name = 'new_name'
new_folder_path = os_util.joinPaths([home_path, new_folder_name])
os_util.ensureDir(new_folder_path)

# Get Memory and CPU stats
os_util.getComputerStats()
```

### Date utility

To be used for standardizing types, formats and timezones for all variables representing a date/time. 
It takes in any of [datetime, str, float or pd.Timestamp] and returns a datetime.datetime object

```python
import dqcpy.utility.dt_util as dt_util

d1 = '2021-01-01'
d2 = '2021/01/01'
d3 = '2021.01.01'

print(dt_util.toDatetime(d1) == dt_util.toDatetime(d2) == dt_util.toDatetime(d3))

print(dt_util.CURRENT_TIMEZONE)
print(dt_util.getCurrentTimeMicros('UTC'))
print(dt_util.getCurrentTimeMicros('EST'))
print(dt_util.getCurrentDatetime('UTC'))

```
