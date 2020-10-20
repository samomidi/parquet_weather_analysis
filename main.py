import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob
import dask.dataframe as dd


def read_csv(filename):
    return pd.read_csv(
        filename
    )

files = glob.glob("data/*.csv")


dfs = list(map(read_csv, files))

# Use the first table to create schema for the writer
table = pa.Table.from_pandas(dfs[0], preserve_index=False)
writer = pq.ParquetWriter('weather-rowgroups.parquet', table.schema)

for df in dfs:
    table = pa.Table.from_pandas(df, preserve_index=False)
    writer.write_table(table)
writer.close()


# Some analysis on the parquet file and its row groups to identify characteristics of our data structure
filename = "weather-rowgroups.parquet"
pq_file = pq.ParquetFile(filename)

data = []
for rg in range(pq_file.metadata.num_row_groups):
    rg_meta = pq_file.metadata.row_group(rg)
    data.append([rg, rg_meta.num_rows, rg_meta.total_byte_size])

# To get number of rows and cols
pq_file.metadata.num_rows
pq_file.metadata.num_columns

# To get metadata of column
rg_meta.column(7)


# Find min and max statistics of a column for each row group
column = 7
data = []

for rg in range(pq_file.metadata.num_row_groups):
    rg_meta = pq_file.metadata.row_group(rg)
    data.append([rg, rg_meta.column(column).statistics.min, rg_meta.column(column).statistics.max])
print(data)
data_df = pd.DataFrame(data, columns=["rowgroup", "min", "max"])

max_temp = data_df.loc[data_df["max"].idxmax()][2]




rg_meta.column(column).statistics.max

df = dd.read_parquet("weather-rowgroups.parquet", columns=['ObservationDate', 'Region', 'ScreenTemperature'])
df = df[df.ScreenTemperature == max_temp]
print(df.compute())