# Utility

### OS Utility

```python
import smartpy.utility.os_util as os_util

# Creating a new folder
home_path = os_util.getUserHomePath()
new_folder_name = 'new_name'
new_folder_path = os_util.joinPaths([home_path, new_folder_name])
os_util.ensureDir(new_folder_path)

# Get Memory and CPU stats
os_util.getComputerStats()
```

### Date utility

To be used for standardizing types, formats and timezones for all variables representing a date/time. It takes in any
of [datetime, str, float or pd.Timestamp] and returns a datetime.datetime object

```python
import smartpy.utility.dt_util as dt_util

d1 = '2021-01-01'
d2 = '2021/01/01'
d3 = '2021.01.01'

print(dt_util.toDatetime(d1) == dt_util.toDatetime(d2) == dt_util.toDatetime(d3))

print(dt_util.CURRENT_TIMEZONE)
print(dt_util.getCurrentTimeMicros('UTC'))
print(dt_util.getCurrentTimeMicros('EST'))
print(dt_util.getCurrentDatetime('UTC'))

```


## Data updater
```python
import pandas as pd
from smartpy.data.data_updater import DataUpdater

data_updater = DataUpdater(id_column_name='timestamp',
                           source_file_path="s3://erere",
                           data_source='s3')

new_data = pd.DataFrame
data_updater.inputNewData(new_data)
data_updater.getIDsAlreadyPresent(partition_filter=lambda x: x["coin"] == symbol)
data_updater.getIDsToUpload()
data_updater.saveNewData(partition_cols=['coin'])

```






