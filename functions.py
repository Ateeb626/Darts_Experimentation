import pandas as pd
import holidays
from datetime import datetime, timedelta
from sklearn.preprocessing import LabelEncoder


def data_amputation(df):
    # Group by product_item_sku_id and resampling day-wise, then forward fill
    feature_daily_base_price_df = df.groupby('product_item_sku_id')['base_price'].resample(
        'D').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_sale_price_df = df.groupby('product_item_sku_id')['sale_price'].resample(
        'D').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_quantity_df = df.groupby('product_item_sku_id')['sales'].resample(
        'D').sum().groupby('product_item_sku_id').fillna(0).reset_index()
    feature_daily_list_price_df = df.groupby('product_item_sku_id')['list_price'].resample(
        'D').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_msrp_price_df = df.groupby('product_item_sku_id')['msrp'].resample(
        'D').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_views_df = df.groupby('product_item_sku_id')['views'].resample(
        'D').sum().groupby('product_item_sku_id').fillna(0).reset_index()
    feature_daily_carts_df = df.groupby('product_item_sku_id')['cart_quantity'].resample(
        'D').sum().groupby('product_item_sku_id').fillna(0).reset_index()

    # Compiling Resampled Data
    data = pd.DataFrame(
        {
            'creation_date': feature_daily_base_price_df['creation_date'],
            'product_item_sku_id': feature_daily_base_price_df['product_item_sku_id'],
            'sales': feature_daily_quantity_df['sales'],
            'base_price': feature_daily_base_price_df['base_price'],
            'list_price': feature_daily_list_price_df['list_price'],
            'sale_price': feature_daily_sale_price_df['sale_price'],
            'msrp': feature_daily_msrp_price_df['msrp'],
            'views': feature_daily_views_df['views'],
            'cart_quantity': feature_daily_carts_df['cart_quantity']
        })

    return data


def data_amputation_weekly(df):
    # Group by product_item_sku_id and resampling day-wise, then forward fill
    feature_daily_base_price_df = df.groupby('product_item_sku_id')['base_price'].resample(
        'W').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_sale_price_df = df.groupby('product_item_sku_id')['sale_price'].resample(
        'W').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_quantity_df = df.groupby('product_item_sku_id')['sales'].resample(
        'W').sum().groupby('product_item_sku_id').fillna(0).reset_index()
    feature_daily_list_price_df = df.groupby('product_item_sku_id')['list_price'].resample(
        'W').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_msrp_price_df = df.groupby('product_item_sku_id')['msrp'].resample(
        'W').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_views_df = df.groupby('product_item_sku_id')['views'].resample(
        'W').sum().groupby('product_item_sku_id').fillna(0).reset_index()
    feature_daily_carts_df = df.groupby('product_item_sku_id')['cart_quantity'].resample(
        'W').sum().groupby('product_item_sku_id').fillna(0).reset_index()

    # Compiling Resampled Data
    data = pd.DataFrame(
        {
            'creation_date': feature_daily_base_price_df['creation_date'],
            'product_item_sku_id': feature_daily_base_price_df['product_item_sku_id'],
            'sales': feature_daily_quantity_df['sales'],
            'base_price': feature_daily_base_price_df['base_price'],
            'list_price': feature_daily_list_price_df['list_price'],
            'sale_price': feature_daily_sale_price_df['sale_price'],
            'msrp': feature_daily_msrp_price_df['msrp'],
            'views': feature_daily_views_df['views'],
            'cart_quantity': feature_daily_carts_df['cart_quantity']
        })

    return data


def data_amputation_three_days(df):
    # Group by product_item_sku_id and resampling day-wise, then forward fill
    feature_daily_base_price_df = df.groupby('product_item_sku_id')['base_price'].resample(
        '3D').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_sale_price_df = df.groupby('product_item_sku_id')['sale_price'].resample(
        '3D').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_quantity_df = df.groupby('product_item_sku_id')['sales'].resample(
        '3D').sum().groupby('product_item_sku_id').fillna(0).reset_index()
    feature_daily_list_price_df = df.groupby('product_item_sku_id')['list_price'].resample(
        '3D').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_msrp_price_df = df.groupby('product_item_sku_id')['msrp'].resample(
        '3D').mean().groupby('product_item_sku_id').ffill().reset_index()
    feature_daily_views_df = df.groupby('product_item_sku_id')['views'].resample(
        '3D').sum().groupby('product_item_sku_id').fillna(0).reset_index()
    feature_daily_carts_df = df.groupby('product_item_sku_id')['cart_quantity'].resample(
        '3D').sum().groupby('product_item_sku_id').fillna(0).reset_index()

    # Compiling Resampled Data
    data = pd.DataFrame(
        {
            'creation_date': feature_daily_base_price_df['creation_date'],
            'product_item_sku_id': feature_daily_base_price_df['product_item_sku_id'],
            'sales': feature_daily_quantity_df['sales'],
            'base_price': feature_daily_base_price_df['base_price'],
            'list_price': feature_daily_list_price_df['list_price'],
            'sale_price': feature_daily_sale_price_df['sale_price'],
            'msrp': feature_daily_msrp_price_df['msrp'],
            'views': feature_daily_views_df['views'],
            'cart_quantity': feature_daily_carts_df['cart_quantity']
        })

    return data


def preprocessing_data(df, site_id, frequency):

    # Lowering Case Column Names
    df.columns = [col.lower() for col in df.columns]
    # label_encoder=LabelEncoder()

    # Mapping Column Names
    column_mappings = {
        'creation_date': 'creation_date',
        'product_item_sku_id': 'product_item_sku_id',
        'quantity': 'sales',
        'base_price': 'base_price',
        'listprice': 'list_price',
        'saleprice': 'sale_price',
        'msrp': 'msrp',
        'views': 'views',
        'cart_quantity': 'cart_quantity',
        'siteid': 'site_id'
    }
    df = df.rename(columns=column_mappings)

    df = df[df['site_id'] == site_id].copy()

    # Filtering Useful Columns
    df = df[[
        'creation_date',
        'product_item_sku_id',
        'base_price',
        'sales',
        'list_price',
        'sale_price',
        'msrp',
        'views',
        'cart_quantity'
    ]]

    # Formatting Date
    df['creation_date'] = pd.to_datetime(df['creation_date'])
    df['creation_date'] = df['creation_date'].dt.date
    df.set_index('creation_date', inplace=True)
    df.sort_index(ascending=True, inplace=True)
    df.index = pd.to_datetime(df.index)
    # df['product_item_sku_id'] = label_encoder.fit_transform(df['product_item_sku_id'])
    if frequency == 0:
        data = data_amputation(df)
    elif frequency == 7:
        data = data_amputation_weekly(df)
    elif frequency == 3:
        data = data_amputation_three_days(df)

    data.to_csv('data.csv', index=False, mode='w')
    return data


def filter_on_base_price_change(df, threshold_base_price_change):
    # Identify changes in the base price for each product_item_sku_id
    df['base_price_change'] = df.groupby('product_item_sku_id')[
        'base_price'].diff().ne(0)

    # Count how many times the base price changed for each product_item_sku_id
    change_counts = df.groupby('product_item_sku_id')[
        'base_price_change'].sum().reset_index(name='no_of_price_changes')

    # Identify product_item_sku_ids where the base price changed at least 3 times
    sku_ids_to_filter = change_counts[change_counts['no_of_price_changes'] < (
        threshold_base_price_change + 1)]['product_item_sku_id']

    # Filter out rows for the identified product_item_sku_ids
    filtered_df = df[~df['product_item_sku_id'].isin(sku_ids_to_filter)]

    return filtered_df


def filter_on_minimum_sales(df, threshold_minimum_sales):
    total_sales_per_sku = df.groupby('product_item_sku_id')[
        'sales'].sum().reset_index()
    skus_above_threshold = total_sales_per_sku[total_sales_per_sku['sales']
                                               >= threshold_minimum_sales]

    filtered_df_sales = df[df['product_item_sku_id'].isin(
        skus_above_threshold['product_item_sku_id'])]

    filtered_df_sales.to_csv('thresholding_final.csv', index=False, mode='w')

    return filtered_df_sales


def filter_on_recent_months(df, threshold_recent_months):
    # Convert 'creation_date' to datetime format
    df['creation_date'] = pd.to_datetime(
        df['creation_date'], format='%Y-%m-%d')

    # Calculate the threshold date
    threshold_date = datetime.now() - timedelta(days=threshold_recent_months * 30)

    # Filter out SKUs with the latest sale being more than threshold months from system date
    filtered_skus = df.groupby('product_item_sku_id')[
        'creation_date'].max() > threshold_date
    filtered_skus = filtered_skus[filtered_skus].index.tolist()

    filtered_dataframe = df[df['product_item_sku_id'].isin(filtered_skus)]

    return filtered_dataframe


def threshold_filtering_price_optimization(df, threshold_base_price_change, threshold_minimum_sales, threshold_recent_months, threshold_minimum_ratio_of_days_sale_total):

    filtered_df = filter_on_base_price_change(df, threshold_base_price_change)

    filtered_df_sales = filter_on_minimum_sales(
        filtered_df, threshold_minimum_sales)

    filtered_df_months = filter_on_recent_months(
        filtered_df_sales, threshold_recent_months)

    filtered_df_months.to_csv('thresholding_final.csv', index=False, mode='w')

    return filtered_df_months


def feature_engineering(df):

    df = df.reset_index(drop=True)
    df.info()
    # Add a column for holidays
    us_holidays = holidays.UnitedStates()
    df['is_holiday'] = [
        date in us_holidays for date in df['creation_date']]

    # Assuming 'df' is your DataFrame
    df['is_holiday'] = df['is_holiday'].map({False: 0, True: 1})

    df['creation_date'] = pd.to_datetime(df['creation_date'])
    df['day_of_week'] = df['creation_date'].dt.dayofweek
    df['week_of_month'] = df['creation_date'].apply(lambda x: (x.day-1)//7 + 1)
    df['month_of_year'] = df['creation_date'].dt.month

    # Add days till specific events

    def days_till_event(event_date, creation_date):
        event_month_day = event_date[1:]
        event_date = pd.to_datetime(f"{creation_date.year}-{event_month_day}")
        if creation_date > event_date:
            event_date = pd.to_datetime(
                f"{creation_date.year + 1}-{event_month_day}")
        return (event_date - creation_date).days
    # add days till labor day, 4th of july, thanksgiving, newyears

    df['days_till_black_friday'] = df['creation_date'].apply(
        lambda x: days_till_event('-11-24', x))
    df['days_till_christmas'] = df['creation_date'].apply(
        lambda x: days_till_event('-12-25', x))
    df['days_till_summer'] = df['creation_date'].apply(
        lambda x: days_till_event('-06-01', x))
    df['days_till_winter'] = df['creation_date'].apply(
        lambda x: days_till_event('-12-01', x))
    df['is_promotion'] = df['sale_price'].notna().astype(int)
    # Calculate profit margin assuming 100% when base_price is half of msrp
    df['margin'] = ((df['msrp'] - df['base_price']) / (df['msrp'] / 2)) * 100
    # Print the updated DataFrame
    df['creation_date'] = df['creation_date'].dt.strftime('%Y-%m-%d')
    # df.to_csv('feature_engineering.csv', index=False, mode='w')

    df.to_csv('feature_engineering.csv', index=False, mode='w')

    return df


def train_test_split_last_n_rows(df, n):
    # Sort the dataframe by 'product_item_sku_id' and 'creation_date'

    # Define a function to split the last n rows for each product
    def split_last_n_rows_train(group):
        return group.iloc[:-n]

    def split_last_n_rows_test(group):
        return group.iloc[-n:]

    # Apply the split function to each product group
    train_df = df.groupby('product_item_sku_id', group_keys=False).apply(
        split_last_n_rows_train)

    test_df = df.groupby('product_item_sku_id', group_keys=False).apply(
        split_last_n_rows_test)

    train_df = train_df.groupby('product_item_sku_id', group_keys=False).apply(
        lambda group: group.reset_index(drop=True))
    return train_df, test_df
