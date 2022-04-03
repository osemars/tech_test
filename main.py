import pandas as pd
import statistics as s

# locations
file_loc = "C:/Users/Ai2sh/Downloads/tech_test/tech_test/data/"
historical_loc = file_loc + "historical.csv"
test_7_loc = file_loc + "test_7.csv"
device_loc = file_loc + "device.json"

# reading data
device_json = pd.read_json(device_loc)
test_7_df = pd.read_csv(test_7_loc)
historical_df = pd.read_csv(historical_loc)

# working with .json
with open(device_loc, encoding='utf-8') as inputfile:
    device_df = pd.read_json(inputfile)

#converting .json to .csv
device_df.to_csv('csvfile.csv', encoding='utf-8', index=False)
device_df = device_df.T
device_df =device_df.reset_index()
device_df = device_df.rename(columns={'index': 'device_id'})

print(device_df)
print(historical_df.head(10))
print(test_7_df.head(10))

# print(historical_df.count())
# print(test_7_df.count())
print("column names and types for historical_df: ")
for column in historical_df.columns:
    print(column, historical_df[column].dtype.name)

print('\n' + "Column names and types for test_7")
for column in test_7_df.columns:
    print(column, test_7_df[column].dtype.name)

#making a timestamp column on historical
historical_df['timestamp'] = pd.to_datetime(historical_df['date'] + ' ' + historical_df['time'])
#dropping unnecessary columns
historical_df.drop(['date', 'time'], axis=1, inplace=True)
print(historical_df.head(5))

#converting the timestamp for test_7_df to yyyy-mm-dd hh:mm:ss
test_7_df['timestamp']= pd.to_datetime(test_7_df['timestamp'])
test_7_df['timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
print(test_7_df.head(5))

#dropping unnecessary columns from test_7_df
test_7_df.drop(['channel', 'voltage', 'power_factor', 'apparent_power', 'state', 'collection_time', 'device'], axis=1, inplace=True)
test_7_df = test_7_df.rename(columns={'socket_id': 'device_id'})
print(test_7_df.head(5))

print("column names and types for historical_df: ")
for column in historical_df.columns:
    print(column, historical_df[column].dtype.name)

print('\n' + "Column names and types for test_7")
for column in test_7_df.columns:
    print(column, test_7_df[column].dtype.name)

# union of test_7 and historical datasets
print("Total number of expected rows: ")
print(len(historical_df.index) + len(test_7_df.index))

concatenated_df = pd.concat([historical_df, test_7_df])
print(concatenated_df)

# joining concatenated to device_df
final_df = pd.merge(concatenated_df, device_df, how="left", on=["device_id"])
# print(final_df)

# get unique devices and count
devices = final_df['device_type'].unique()
print(final_df.device_type.value_counts())
print(devices)

# anonymize device_id using uuid
# import uuid
# devices_list=[]
# devices = final_df['uuid'].unique()
# for i in range(len(list(devices))):
#     devices_list.append(uuid.uuid4())
# print (devices_list)
#['test_1':'bc86aaec-8a88-48d4-a6cf-23405b4c4dfb', 'test_2':'88e0d1dd-304f-4187-b34f-5643ae172c16', 'test_3':'6c65bde6-4dbf-4885-932e-1cc9045de431',
# 'test_4':'2b7497cd-d705-45b7-9b1a-a58d0e92d3a6', 'test_5':'2177db4f-3df5-4984-8a37-b6080a2956e5', 'test_6':'29ef5676-6f02-4f09-b160-fb029b8fd077',
# 'test_7':'9be3e15f-b263-47fe-afa5-d654fc8e885a']

def anonymize_test(device_id):
    return {
        'test_1':'bc86aaec-8a88-48d4-a6cf-23405b4c4dfb',
        'test_2':'88e0d1dd-304f-4187-b34f-5643ae172c16',
        'test_3':'6c65bde6-4dbf-4885-932e-1cc9045de431',
        'test_4':'2b7497cd-d705-45b7-9b1a-a58d0e92d3a6',
        'test_5':'2177db4f-3df5-4984-8a37-b6080a2956e5',
        'test_6':'29ef5676-6f02-4f09-b160-fb029b8fd077',
        'test_7':'9be3e15f-b263-47fe-afa5-d654fc8e885a'
    }[device_id]

final_df['device_id'] = final_df['device_id'].apply(anonymize_test)
print(final_df.head(5))
print(final_df.shape)
# print(list(final_df.columns.values))

#search for na values
print(final_df.isna().sum())

#search for high unusual values in sensor columns
print('\nchecking for the highest values in sensor columns')
def max_err_for_mean(column_name):
    # max_val = final_df[[column_name]].max()
    # print(max_val[0])
    max_val_id = final_df[[column_name]].idxmax()
    # geting max error index
    max_val_id = max_val_id[0]
    # geting max error index for rows before and after
    column_name_a = final_df.at[max_val_id - 1, column_name]
    column_name_z = final_df.at[max_val_id + 1, column_name]
    # getting average of values before and after
    column_name_mean = s.mean([column_name_a, column_name_z])
    # replacing error value with average
    final_df.at[max_val_id, column_name] = column_name_mean
    print("new value: ")
    print(final_df.at[max_val_id, column_name])
    print("new max: ")
    print(final_df[[column_name]].max())


#search for lowest unusual value in sensor columns
def min_err_for_mean(column_name):
    # min_val = final_df[[column_name]].min()
    # print(min_val[0])
    min_val_id = final_df[[column_name]].idxmin()
    #geting min error index
    min_val_id = min_val_id[0]
    # geting min error index for rows before and after
    column_name_a = final_df.at[min_val_id - 1, column_name]
    column_name_z = final_df.at[min_val_id + 1, column_name]
    # getting average of values before and after
    column_name_mean = s.mean([column_name_a, column_name_z])
    # replacing error value with average
    final_df.at[min_val_id, column_name] = column_name_mean
    print("new value: ")
    print(final_df.at[min_val_id, column_name])
    print("new min: ")
    print(final_df[[column_name]].min())

# what if there are more than one values with < -1000?
def find_min_errs(column_name):
    for i in range(final_df.shape[0]):
        min_val = final_df[[column_name]].min()
        print(min_val)
        if (min_val[0] < -10000):
            min_err_for_mean(column_name)
        else:
            break

# what if there are more than one values with > 1000?
def find_max_errs(column_name):
    for i in range(final_df.shape[0]):
        max_val = final_df[[column_name]].max()
        print(max_val)
        if (max_val[0] > 10000):
            max_err_for_mean(column_name)
        else:
            break


find_max_errs('current')
find_max_errs('active_power')
find_max_errs('reactive_power')

find_min_errs('current')
find_min_errs('active_power')
find_min_errs('reactive_power')


#drop dublicate rows where 'timestamp', 'current', 'active_power', 'reactive_power', 'ts_id' are exactly same
final_df = final_df.drop_duplicates(subset=['timestamp', 'current', 'active_power', 'reactive_power', 'ts_id'], keep='first')
# print(final_df)
print(final_df.shape)
# final_df.to_csv(r'C:/Users/Ai2sh/Downloads/tech_test/tech_test/data/final_df2.csv', sep=',', header=True)

#final_df.to_csv(r'C:/Users/Ai2sh/Downloads/tech_test/tech_test/data/final_df.csv', sep=',', header=True)

# t = d.index
#r = pd.date_range(d['timestamp'].min(), periods=209*3, freq='0.33S')
# d = d.reindex(d['timestamp'].union(r)).interpolate('index').loc[r]

# final_df['dates'] = pd.to_datetime(final_df['timestamp']).dt.date
# final_df['time'] = pd.to_datetime(final_df['timestamp']).dt.time
# print(final_df)

#Tried resampling and filling the na with interpolation but it kept producing massive .csv files
# final_df.set_index('timestamp', inplace=True)
# print(final_df.index)
# final_df_resampled = final_df[['current', 'active_power', 'reactive_power']].resample("0.3S").mean()
# # final_df_interp = final_df.interpolate(method='linear', periods=209*3)
# print(final_df_resampled.head(30))
# print(final_df_resampled.shape)
# final_df_resampled.to_csv(r'C:/Users/Ai2sh/Downloads/tech_test/tech_test/data/final_df2.csv', sep=',', header=True)
final_df.to_csv(r'C:/Users/Ai2sh/Downloads/tech_test/tech_test/data/results.csv', sep=',', header=True, index=False)

#plotting linegraph
import matplotlib.pyplot as plt
final_df['active_power'].plot()
plt.show()
