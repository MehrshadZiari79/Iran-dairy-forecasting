import pandas as pd
from persiantools.jdatetime import JalaliDate

pd.set_option('future.no_silent_downcasting', True)

# =====================================================
# 1️⃣  MILK PRODUCTION
# =====================================================

df_milk_raw = pd.read_excel('Milk_Production_Seasonal.xlsx', header=None)

df_milk = df_milk_raw.iloc[:, [1, 2, 3]].copy()
df_milk.columns = ['Year_SH', 'Season', 'Milk_Production']

df_milk = df_milk[pd.to_numeric(df_milk['Year_SH'], errors='coerce').notna()]
df_milk['Year_SH'] = df_milk['Year_SH'].astype(int)
df_milk['Milk_Production'] = pd.to_numeric(df_milk['Milk_Production'], errors='coerce')

df_milk['Season'] = df_milk['Season'].astype(str).str.strip()

season_to_month = {
    'بهار': 1,
    'تابستان': 4,
    'پاییز': 7,
    'زمستان': 10
}

df_milk['Month_SH'] = df_milk['Season'].map(season_to_month)
df_milk = df_milk[df_milk['Month_SH'].notna()]
df_milk['Month_SH'] = df_milk['Month_SH'].astype(int)

df_milk['Date_Gregorian'] = df_milk.apply(
    lambda row: JalaliDate(row['Year_SH'], row['Month_SH'], 1).to_gregorian(),
    axis=1
)

df_milk['Date_Gregorian'] = pd.to_datetime(df_milk['Date_Gregorian'])
df_milk = df_milk[['Date_Gregorian', 'Milk_Production']]

# =====================================================
# 2️⃣  CPI  (ماه فارسی → عدد)
# =====================================================

df_cpi_raw = pd.read_excel('CPI.xlsx', sheet_name='جدول 1', header=None)

years_row = df_cpi_raw.iloc[1].ffill()
months_row = df_cpi_raw.iloc[2]
data_rows = df_cpi_raw.iloc[3:].reset_index(drop=True)

# مپ ماه فارسی به عدد
month_map = {
    'فروردین': 1,
    'اردیبهشت': 2,
    'خرداد': 3,
    'تیر': 4,
    'مرداد': 5,
    'شهریور': 6,
    'مهر': 7,
    'آبان': 8,
    'آذر': 9,
    'دی': 10,
    'بهمن': 11,
    'اسفند': 12
}

data_list = []

for col in range(1, df_cpi_raw.shape[1]):
    year = years_row[col]
    month_name = str(months_row[col]).strip()

    if pd.notna(year) and month_name in month_map:

        month_number = month_map[month_name]

        for row in range(len(data_rows)):
            category = str(data_rows.iloc[row, 0]).strip()
            value = data_rows.iloc[row, col]

            # تبدیل امن به عدد
            value_numeric = pd.to_numeric(value, errors='coerce')

            if pd.notna(value_numeric):
                data_list.append([
                    int(year),
                    month_number,
                    category,
                    float(value_numeric)
                ])


df_cpi = pd.DataFrame(
    data_list,
    columns=['Year_SH', 'Month_SH', 'Category', 'CPI_Value']
)

wanted_categories = [
    'شاخص كل',
    '011 -  خوراكيها',
    '0114 -      شير، پنير و تخم مرغ'
]

df_cpi = df_cpi[df_cpi['Category'].isin(wanted_categories)]

df_cpi['Date_Gregorian'] = df_cpi.apply(
    lambda row: JalaliDate(row['Year_SH'], row['Month_SH'], 1).to_gregorian(),
    axis=1
)

df_cpi['Date_Gregorian'] = pd.to_datetime(df_cpi['Date_Gregorian'])

df_cpi_pivot = df_cpi.pivot_table(
    index='Date_Gregorian',
    columns='Category',
    values='CPI_Value'
).reset_index()

# =====================================================
# 3️⃣  GOOGLE TRENDS
# =====================================================

df_trends = pd.read_csv('google_trends.csv')

df_trends['Date_Gregorian'] = pd.to_datetime(df_trends['Time'])
df_trends.drop(columns=['Time'], inplace=True)

# =====================================================
# 4️⃣  MERGE ALL
# =====================================================

df_merge1 = pd.merge(df_milk, df_trends, on='Date_Gregorian', how='outer')
df_final = pd.merge(df_merge1, df_cpi_pivot, on='Date_Gregorian', how='outer')

df_final = df_final.sort_values('Date_Gregorian').reset_index(drop=True)

# =====================================================
# 5️⃣  SAVE
# =====================================================

df_final.to_excel('Final_Dataset.xlsx', index=False)

print("✅ Final dataset created successfully.")
print(df_final.head())
