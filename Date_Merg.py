import pandas as pd
from persiantools.jdatetime import JalaliDate

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

print("========== LOADING MILK DATA ==========")
df_milk_raw = pd.read_excel('Milk_Production_Seasonal.xlsx', header=None)
df_milk = df_milk_raw.iloc[:, [1,2,3]].copy()
df_milk.columns = ['Year_SH', 'Season', 'Milk_Production']

df_milk = df_milk[pd.to_numeric(df_milk['Year_SH'], errors='coerce').notna()]
df_milk['Year_SH'] = df_milk['Year_SH'].astype(int)
df_milk['Milk_Production'] = pd.to_numeric(df_milk['Milk_Production'], errors='coerce')
df_milk['Season'] = df_milk['Season'].astype(str).str.strip()

season_to_months = {
    'بهار':[1,2,3], 'تابستان':[4,5,6],
    'پاییز':[7,8,9], 'زمستان':[10,11,12]
}

milk_monthly_records = []
for _, row in df_milk.iterrows():
    months = season_to_months.get(row['Season'], [])
    if months:
        monthly_value = row['Milk_Production']/len(months)
        for m in months:
            milk_monthly_records.append([row['Year_SH'], m, monthly_value])

df_milk_monthly = pd.DataFrame(milk_monthly_records, columns=['Year_SH','Month_SH','Milk_Production'])
df_milk_monthly['Date_Gregorian'] = df_milk_monthly.apply(
    lambda r: pd.Timestamp(JalaliDate(int(r['Year_SH']), int(r['Month_SH']), 1).to_gregorian()), axis=1
)

print(f"Milk Date Range: {df_milk_monthly['Date_Gregorian'].min()} to {df_milk_monthly['Date_Gregorian'].max()}")
print(f"Milk rows: {len(df_milk_monthly)}")

# ---------------- CPI ----------------
print("\n========== LOADING CPI DATA ==========")
df_cpi_raw = pd.read_excel('CPI.xlsx', sheet_name='جدول 1', header=None)
years_row = df_cpi_raw.iloc[1].ffill()
months_row = df_cpi_raw.iloc[2]
data_rows = df_cpi_raw.iloc[3:].reset_index(drop=True)

month_map = {'فروردین':1,'اردیبهشت':2,'خرداد':3,
             'تیر':4,'مرداد':5,'شهریور':6,
             'مهر':7,'آبان':8,'آذر':9,
             'دی':10,'بهمن':11,'اسفند':12}

data_list=[]
for col in range(1, df_cpi_raw.shape[1]):
    year = years_row[col]
    month_name = str(months_row[col]).strip()
    if pd.notna(year) and month_name in month_map:
        month_number = month_map[month_name]
        for row in range(len(data_rows)):
            category = str(data_rows.iloc[row,0]).strip()
            value = pd.to_numeric(data_rows.iloc[row,col], errors='coerce')
            if pd.notna(value):
                data_list.append([int(year), month_number, category, value])

df_cpi = pd.DataFrame(data_list, columns=['Year_SH','Month_SH','Category','CPI_Value'])
wanted_categories = ['شاخص كل','011 -  خوراكيها','0114 -      شير، پنير و تخم مرغ']
df_cpi = df_cpi[df_cpi['Category'].isin(wanted_categories)]
df_cpi['Date_Gregorian'] = df_cpi.apply(
    lambda r: pd.Timestamp(JalaliDate(int(r['Year_SH']), int(r['Month_SH']),1).to_gregorian()), axis=1
)
df_cpi_pivot = df_cpi.pivot_table(index='Date_Gregorian', columns='Category', values='CPI_Value').reset_index()
print(f"CPI Date Range: {df_cpi_pivot['Date_Gregorian'].min()} to {df_cpi_pivot['Date_Gregorian'].max()}")
print(f"CPI rows: {len(df_cpi_pivot)}")

# ---------------- GOOGLE TRENDS ----------------
print("\n========== LOADING GOOGLE TRENDS ==========")
df_trends = pd.read_csv('google_trends.csv')
df_trends['Date_Gregorian'] = pd.to_datetime(df_trends['Time']).dt.to_period('M').dt.to_timestamp()
df_trends.drop(columns=['Time'], inplace=True)
df_trends_monthly = df_trends.groupby('Date_Gregorian').mean().reset_index()
print(f"Trends Date Range: {df_trends_monthly['Date_Gregorian'].min()} to {df_trends_monthly['Date_Gregorian'].max()}")
print(f"Trends rows: {len(df_trends_monthly)}")

# ---------------- MERGE ALL ----------------
print("\n========== MERGING ALL MONTHLY DATA ==========")
df_merge = pd.merge(df_milk_monthly, df_trends_monthly, on='Date_Gregorian', how='outer')
df_final = pd.merge(df_merge, df_cpi_pivot, on='Date_Gregorian', how='outer')

# Sort, reset index, interpolate numeric
df_final = df_final.sort_values('Date_Gregorian').reset_index(drop=True)
numeric_cols = df_final.select_dtypes(include='number').columns.tolist()
df_final[numeric_cols] = df_final[numeric_cols].interpolate(method='linear', limit_direction='both')

print(f"After Full Merge: {len(df_final)} rows")
print("\n✅ Clean national monthly dataset created successfully.")
print(df_final.head(10))

df_final.to_excel('Final_Dataset_Monthly_Filled.xlsx', index=False)