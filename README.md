import pandas as pd
from datetime import datetime
import os
import glob
import numpy as np

# Date for output file-----
today_date = datetime.today().strftime("%d-%b-%Y")

# Load discontinued debtor lists
df1 = pd.read_csv(
    r"D:\Pywar\Renewal Project\Data list\Discontinued Debtor List.csv",
    dtype=str
)

df2 = pd.read_csv(
    r"D:\Pywar\Renewal Project\Data list\Discontinued Debtor List (1).csv",
    dtype=str
)

# Load HTML tables from Excel
dfm1 = pd.read_html(
    r"D:\Pywar\Renewal Project\tableExport.xls"
)[0].astype(str)

dfm2 = pd.read_html(
    r"D:\Pywar\Renewal Project\tableExport (1).xls"
)[0].astype(str)

dfm3 = pd.read_html(
    r"D:\Pywar\Renewal Project\tableExport (2).xls"
)[0].astype(str)

# Load already processed renewal files
folder_path = r"D:\Pywar\Renewal Project\Renewal Done"
excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))

if excel_files:
    df_list = [pd.read_excel(file, dtype=str) for file in excel_files]
    df_all = pd.concat(df_list, ignore_index=True)
else:
    df_all = pd.DataFrame(columns=['Id'])


# Combine all main tables
dfm = pd.concat([dfm1, dfm2, dfm3], ignore_index=True)

# Create merge key
dfm['Id Name'] = dfm['Id'].str.strip() + ' ' + dfm['Client Name'].str.strip()

# Combine discontinued lists
dff = pd.concat([df1, df2], ignore_index=True)

# Create merge key
dff['Id Name'] = dff['Char Id'].str.strip() + ' ' + dff['Client Name'].str.strip()


# Left merge
dfmerge = pd.merge(
    dfm,
    dff,
    on='Id Name',
    how='left'
)

# Drop unwanted columns safely
dfmerge.drop(
    columns=[
        'Char Id',
        'Unnamed: 0',
        'District',
        'F.A.',
        '2025-11',
        '2025-10',
        'old',
        'Person Name',
        'Person Mobile',
        'Govt/Pvt',
        'Status',
        'address',
        'Client Discontinued Date',
        'Unnamed: 19',
        'Client Name_y'
    ],
    inplace=True,
    errors='ignore'
)

# Filter specific districts (KEEP only matching)

pattern = (
    r'\(Araria\)|\(Banka\)|\(Begusarai\)|\(BhagalPur\)|\(Jamui\)|'
    r'\(Katihar\)|\(Khagaria\)|\(Kishanganj\)|\(Lakhisarai\)|'
    r'\(Munger\)|\(Purnia\)|\(Sheikhpura\)|\(Arwal\)|'
    r'\(Aurangabad\)|\(Gaya\)|\(Jehanabad\)|\(Nawada\)'
)

dfmerge = dfmerge[
    dfmerge['Client Name_x'].str.contains(pattern, na=False)
]

# Remove duplicate clients

dfmerge.drop_duplicates(
    subset=['Client Name_x'],
    inplace=True
)

# Drop rows without Last Bill Date

dfmerge.dropna(subset=['Last Bill Date'], inplace=True)

# Normalize Client Id before comparison
dfmerge['Id'] = dfmerge['Id'].astype(str).str.strip()
df_all['Id'] = df_all['Id'].astype(str).str.strip()

# Drop already processed Client Ids
dfmerge = dfmerge[
    ~dfmerge['Id'].isin(df_all['Id'])
]
dfmerge['Sec Diff'] = dfmerge['Security'].astype(int)+dfmerge['Total'].astype(int)

dfmerge['Status'] = np.where(dfmerge['Sec Diff'] < 0, 'Posible','Not Posibe')

dfmerge = dfmerge.sort_values(by='File No', ascending=True)

# Final output check
print(dfmerge.head())
print("Final rows:", len(dfmerge))

# Save output
output_path = fr"D:\Pywar\Renewal Project\Renewal {today_date}.xlsx"
dfmerge.to_excel(output_path, index=False)

