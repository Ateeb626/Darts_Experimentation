import pandas as pd


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


def preprocessing_data(df):

    # Lowering Case Column Names
    df.columns = [col.lower() for col in df.columns]

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
        'cart_quantity': 'cart_quantity'
    }
    df = df.rename(columns=column_mappings)

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

    data = data_amputation(df)

    data.to_csv('data.csv', index=False, mode='w')
    return data


def threshold_filtering(df):

    # Identify product_item_sku_ids where the base price remains the same throughout the data
    sku_ids_to_filter = df.groupby('product_item_sku_id')[
        'base_price'].transform('nunique') == 3

    # Filter out rows for the identified product_item_sku_ids
    filtered_df = df[~sku_ids_to_filter]

    # Display the filtered dataframe
    print(filtered_df)

    filtered_df.to_csv('thresholding2.csv', index=False, mode='w')
    return filtered_df
