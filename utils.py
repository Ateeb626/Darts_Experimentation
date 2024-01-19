import pandas as pd 
import numpy as np
import holidays as holidays

#https://storage.cloud.google.com/dynamic_pricing-data/merged_dynamic_pricing_data.csv
# df = pd.read_csv("merged_dynamic_pricing_data.csv")

# checking for price volatility
def get_top_volatile_skus_mattress(price_changes):

    unique_site_ids = df['SiteId'].unique()

    dataframes = {}

    for site_id in unique_site_ids:
        dataframes[site_id] = df[df['SiteId'] == site_id].copy()


    # computing and testing for the furniture category ID 201cb789-4198-488b-a5eb-4e7df0fb4bee
    df_mattress = dataframes['8d3ea3bc-f65b-4227-9fa6-6fae40e4575a'].copy()
    
    # Extract relevant columns
    df_mattress= df_mattress[['Product_Item_Sku_Id', 'Base_Price']]

    #  Group by SKU and count unique prices
    mattress_sku_price_counts = df_mattress.groupby('Product_Item_Sku_Id')['Base_Price'].nunique().reset_index(name='Price_Change_Count')

    #Filter SKUs with exactly the defined price changes
    skus_with_changes = mattress_sku_price_counts[mattress_sku_price_counts['Price_Change_Count'] >= price_changes]

    #  List of SKUs with the defined price changes
    sku_list = skus_with_changes['Product_Item_Sku_Id'].tolist()

#  Print the result
    print("Returning List of SKU_IDS with more than ", price_changes,"price changes from denver mattress...")


    return sku_list


def get_top_volatile_skus_furniture(price_changes):

    unique_site_ids = df['SiteId'].unique()

    dataframes = {}

    for site_id in unique_site_ids:
        dataframes[site_id] = df[df['SiteId'] == site_id].copy()

    # computing and testing for the furniture category ID 201cb789-4198-488b-a5eb-4e7df0fb4bee
    df_furniture = dataframes['201cb789-4198-488b-a5eb-4e7df0fb4bee'].copy()
    
    # Extract relevant columns
    df_furniture= df_furniture[['Product_Item_Sku_Id', 'Base_Price']]

    #  Group by SKU and count unique prices
    furniture_sku_price_counts = df_furniture.groupby('Product_Item_Sku_Id')['Base_Price'].nunique().reset_index(name='Price_Change_Count')

    #Filter SKUs with exactly the defined price changes
    skus_with_changes = furniture_sku_price_counts[furniture_sku_price_counts['Price_Change_Count'] >= price_changes]

    #  List of SKUs with the defined price changes
    sku_list = skus_with_changes['Product_Item_Sku_Id'].tolist()

#  Print the result
    print("Returning List of SKU_IDS with more than ", price_changes,"price changes from furniture website...")


    return sku_list


def get_top_selling_skus_mattress(threshold):

    unique_site_ids = df['SiteId'].unique()

    dataframes = {}

    for site_id in unique_site_ids:
        dataframes[site_id] = df[df['SiteId'] == site_id].copy()



    # computing and testing for the mattress category ID 8d3ea3bc-f65b-4227-9fa6-6fae40e4575a

    df_mattress = dataframes['8d3ea3bc-f65b-4227-9fa6-6fae40e4575a'].copy()
    product_sales = df_mattress.groupby('Product_Item_Sku_Id')['Quantity'].sum()

    # Sort the products by total sales in descending order
    top_selling_products = product_sales.sort_values(ascending=False)

    # Get the top selling products

    top_selling_products = top_selling_products.head(threshold)
    top_product_ids = top_selling_products.index

    # Print or use the product IDs
    print("Returning the top selling products from denver mattress...")


    return top_product_ids



def get_top_selling_skus_furniture(threshold):

    unique_site_ids = df['SiteId'].unique()

    dataframes = {}

    for site_id in unique_site_ids:
        dataframes[site_id] = df[df['SiteId'] == site_id].copy()


    df_furniture = dataframes['201cb789-4198-488b-a5eb-4e7df0fb4bee'].copy()
    product_sales = df_furniture.groupby('Product_Item_Sku_Id')['Quantity'].sum()

    # Sort the products by total sales in descending order
    top_selling_products = product_sales.sort_values(ascending=False)

    # Get the top selling products

    top_selling_products = top_selling_products.head(threshold)
    top_product_ids = top_selling_products.index

    # Print or use the product IDs
    print("Returning the top selling products from denver mattress...")
    return top_product_ids



def feature_engineering(sku_id):
    # FEATURE ENGINEERING CELL
    # print(df)
    df = pd.read_csv("merged_dynamic_pricing_data.csv")
    feature_sku_data = df
    # test = df
    # test = test[[
    #     'Base_Price','salePrice']]
    
    # print(feature_sku_data)
    feature_sku_data = feature_sku_data[[
        'Creation_Date','Product_Item_Sku_Id', 'Base_Price', 'Quantity','listPrice','salePrice','msrp','views','cart_quantity']]

    feature_sku_data['Creation_Date'] = pd.to_datetime(feature_sku_data['Creation_Date'])
    feature_sku_data['Creation_Date'] = feature_sku_data['Creation_Date'].dt.date
    feature_sku_data.set_index('Creation_Date', inplace=True)
    feature_sku_data.sort_index(ascending=True, inplace=True)
    feature_sku_data.index = pd.to_datetime(feature_sku_data.index)

    # feature_daily_base_price = feature_sku_data['Base_Price'].resample(
    #     'D').mean()
    # feature_daily_base_price_df = pd.DataFrame(
    #     {'Base_Price': feature_daily_base_price})
    # feature_daily_base_price_df['Base_Price'].fillna(
    #     method='ffill', inplace=True)
    
      # feature_daily_quantity_sum = pd.Series(
    #     feature_sku_data['Quantity'].resample('D').sum())
    # feature_daily_quantity_df = pd.DataFrame(
    #     {'Quantity': feature_daily_quantity_sum})
    
    # feature_daily_list_price = feature_sku_data['listPrice'].resample(
    #     'D').mean()
    # feature_daily_list_price_df = pd.DataFrame(
    #     {'listPrice': feature_daily_list_price})
    # feature_daily_list_price_df['listPrice'].fillna(
    #     method='ffill', inplace=True)
    
    # feature_daily_msrp_price = feature_sku_data['msrp'].resample(
    #     'D').mean()
    # feature_daily_msrp_price_df = pd.DataFrame(
    #     {'msrp': feature_daily_msrp_price})
    # feature_daily_msrp_price_df['msrp'].fillna(
    #     method='ffill', inplace=True)

    # feature_daily_views_sum = pd.Series(
    #     feature_sku_data['views'].resample('D').sum())
    # feature_daily_views_df = pd.DataFrame(
    #     {'views': feature_daily_views_sum})
    
    # feature_daily_carts_sum = pd.Series(
    #     feature_sku_data['cart_quantity'].resample('D').sum())
    # feature_daily_carts_df = pd.DataFrame(
    #     {'cart_quantity': feature_daily_carts_sum})
    
      # # Combine DataFrames
    # feature_sku_top1_data = pd.DataFrame(
    #     {'Quantity': feature_daily_quantity_df['Quantity'], 
    #     'Base_Price': feature_daily_base_price_df['Base_Price'],
    #     'listPrice': feature_daily_list_price_df['listPrice'],
    #     'salePrice': feature_daily_sale_price_df['salePrice'],
    #     'msrp': feature_daily_msrp_price_df['msrp'],
    #     'views': feature_daily_views_df['views'],
    #     'cart_quantity': feature_daily_carts_df['cart_quantity']
    #     })

    # # Reset index for the resampled DataFrame
    # feature_sku_top1_data = feature_sku_top1_data.reset_index()
    
    

    # Group by SKUID and resample 'Base_Price' day-wise, then forward fill
    feature_daily_base_price_df = feature_sku_data.groupby('Product_Item_Sku_Id')['Base_Price'].resample('D').mean().groupby('Product_Item_Sku_Id').ffill().reset_index()
    feature_daily_sale_price_df = feature_sku_data.groupby('Product_Item_Sku_Id')['salePrice'].resample('D').mean().groupby('Product_Item_Sku_Id').ffill().reset_index()
    feature_daily_quantity_df = feature_sku_data.groupby('Product_Item_Sku_Id')['Quantity'].resample('D').sum().groupby('Product_Item_Sku_Id').fillna(0).reset_index()
    feature_daily_list_price_df = feature_sku_data.groupby('Product_Item_Sku_Id')['listPrice'].resample('D').mean().groupby('Product_Item_Sku_Id').ffill().reset_index()
    feature_daily_msrp_price_df = feature_sku_data.groupby('Product_Item_Sku_Id')['msrp'].resample('D').mean().groupby('Product_Item_Sku_Id').ffill().reset_index()
    feature_daily_views_df = feature_sku_data.groupby('Product_Item_Sku_Id')['views'].resample('D').sum().groupby('Product_Item_Sku_Id').fillna(0).reset_index()
    feature_daily_carts_df = feature_sku_data.groupby('Product_Item_Sku_Id')['cart_quantity'].resample('D').sum().groupby('Product_Item_Sku_Id').fillna(0).reset_index()
    
    feature_sku_top1_data = pd.DataFrame(
        {
        'creation_date': feature_daily_base_price_df['Creation_Date'],
        'product_item_sku_id' : feature_daily_base_price_df['Product_Item_Sku_Id'],
        'Quantity': feature_daily_quantity_df['Quantity'], 
        'Base_Price': feature_daily_base_price_df['Base_Price'],
        'listPrice': feature_daily_list_price_df['listPrice'],
        'salePrice': feature_daily_sale_price_df['salePrice'],
        'msrp': feature_daily_msrp_price_df['msrp'],
        'views': feature_daily_views_df['views'],
        'cart_quantity': feature_daily_carts_df['cart_quantity']
        })

    # feature_sku_top1_data = feature_sku_top1_data.reset_index()
    
    print("feature_sku_top1_data")
          
    print(feature_sku_top1_data)


    # # Add a column for holidays
    # us_holidays = holidays.UnitedStates()
    # feature_sku_top1_data['is_holiday'] = [
    #     date in us_holidays for date in feature_sku_top1_data['Creation_Date']]

    # # Implement a rolling window (e.g., window size of 3 days)
    # rolling_window_size = 3
    # feature_sku_top1_data['Quantity_Rolling_3'] = feature_sku_top1_data['Quantity'].rolling(
    #     window=rolling_window_size).mean()
    # feature_sku_top1_data['Base_Price_Rolling_3'] = feature_sku_top1_data['Base_Price'].rolling(
    #     window=rolling_window_size).mean()

    # # Implement a rolling window (e.g., window size of 7 days)
    # rolling_window_size = 7
    # feature_sku_top1_data['Quantity_Rolling_7'] = feature_sku_top1_data['Quantity'].rolling(
    #     window=rolling_window_size).mean()
    # feature_sku_top1_data['Base_Price_Rolling_7'] = feature_sku_top1_data['Base_Price'].rolling(
    #     window=rolling_window_size).mean()

    # # Implement a rolling window (e.g., window size of 30 days)
    # rolling_window_size = 30
    # feature_sku_top1_data['Quantity_Rolling_30'] = feature_sku_top1_data['Quantity'].rolling(
    #     window=rolling_window_size).mean()
    # feature_sku_top1_data['Base_Price_Rolling_30'] = feature_sku_top1_data['Base_Price'].rolling(
    #     window=rolling_window_size).mean()


    # # # Create lag features for Base_Price
    # # feature_sku_top1_data['Base_Price_Lag_1'] = feature_sku_top1_data['Base_Price'].shift(1)
    # # feature_sku_top1_data['Base_Price_Lag_3'] = feature_sku_top1_data['Base_Price'].shift(3)
    # # feature_sku_top1_data['Base_Price_Lag_7'] = feature_sku_top1_data['Base_Price'].shift(7)



    # # Assuming 'feature_sku_top1_data' is your DataFrame
    # feature_sku_top1_data['is_holiday'] = feature_sku_top1_data['is_holiday'].map({False: 0, True: 1})

    # feature_sku_top1_data['dayofweek'] = feature_sku_top1_data['Creation_Date'].dt.dayofweek
    # feature_sku_top1_data['weekofmonth'] = feature_sku_top1_data['Creation_Date'].apply(lambda x: (x.day-1)//7 + 1)
    # feature_sku_top1_data['monthofyear'] = feature_sku_top1_data['Creation_Date'].dt.month



    # # Add days till specific events
    # def days_till_event(event_date, creation_date):
    #     event_month_day = event_date[1:]
    #     event_date = pd.to_datetime(f"{creation_date.year}-{event_month_day}")
    #     if creation_date > event_date:
    #         event_date = pd.to_datetime(f"{creation_date.year + 1}-{event_month_day}")
    #     return (event_date - creation_date).days

    # feature_sku_top1_data['daystillblackfriday'] = feature_sku_top1_data['Creation_Date'].apply(lambda x: days_till_event('-11-24', x))
    # feature_sku_top1_data['daystillchristmas'] = feature_sku_top1_data['Creation_Date'].apply(lambda x: days_till_event('-12-25', x))
    # feature_sku_top1_data['daystillsummer'] = feature_sku_top1_data['Creation_Date'].apply(lambda x: days_till_event('-06-01', x))
    # feature_sku_top1_data['daystillwinter'] = feature_sku_top1_data['Creation_Date'].apply(lambda x: days_till_event('-12-01', x))
    # feature_sku_top1_data['isPromotion'] = feature_sku_top1_data['salePrice'].notna().astype(int)
    # # Calculate profit margin assuming 100% when Base_Price is half of msrp
    # feature_sku_top1_data['margin'] = ((feature_sku_top1_data['msrp'] - feature_sku_top1_data['Base_Price']) / (feature_sku_top1_data['msrp'] / 2)) * 100
    # # Print the updated DataFrame
    
    # test.to_csv('test.csv', index=False, mode='w')
    # return feature_daily_base_price_df
    # feature_daily_quantity_df.to_csv('feature_daily_base_price_df.csv', index=False, mode='w')
    # return feature_daily_base_price_df
    #feature_sku_top1_data.to_csv('feature_sku_top1_data.csv', index=False, mode='w')
    return feature_sku_top1_data
    
    

list=feature_engineering("MA-DMDEEQ")
print(list.columns)