import requests
import pandas as pd
from datetime import datetime, timedelta

# personal API request key applied from website bmreports.com
BMRS_APIKey = 'k0tiv5vqkpgptxa'

# set request data settlement date as previous date
Previous_Date = datetime.today() - timedelta(days=1)
Settlement_Date = Previous_Date.strftime('%Y-%m-%d')

# request all periods data for the given settlement date
Settlement_Period = '*'

# set API request link
url_B1770 = f'https://api.bmreports.com/BMRS/B1770/V1?APIKey={BMRS_APIKey}&SettlementDate={Settlement_Date}&Period={Settlement_Period}&ServiceType=xml'
url_B1780 = f'https://api.bmreports.com/BMRS/B1780/V1?APIKey={BMRS_APIKey}&SettlementDate={Settlement_Date}&Period={Settlement_Period}&ServiceType=xml'

# initial dataframes to hold response data
df_B1770 = pd.DataFrame()
df_B1780 = pd.DataFrame()

try:
    # send get request for B1780 data from website, with timeout as 10 seconds
    response_B1780 = requests.get(url_B1780,timeout=10)
    
    # extract data from xml response and save to dataframe, may need to change xpath if format changes from response
    df_B1780 = pd.read_xml(response_B1780.content, xpath=".//item")

    response_B1770 = requests.get(url_B1770,timeout=10)
    df_B1770 = pd.read_xml(response_B1770.content, xpath=".//item")
    
except Exception as e:
    print (e)
    print ('request failed')
    
# do calculation if both dataframe have records, may need to change column names according to response
if df_B1780.shape[0]>0 and df_B1770.shape[0]>0:
    try:  
        # add merge column on df_B1770 to match df_B1780 for merging two dataframe
        df_B1770['imbalanceQuantityDirection'] = ['SURPLUS' if x =='Excess balance' else 'DEFICIT' for x in df_B1770['priceCategory']]
        
        # merge df_B1770 and df_B1780 for calculation
        df_merge = pd.merge(df_B1770[{'settlementDate','settlementPeriod','imbalanceQuantityDirection','imbalancePriceAmountGBP'}],df_B1780[{'settlementDate','settlementPeriod','imbalanceQuantityDirection','imbalanceQuantityMAW'}],on=['settlementDate','settlementPeriod','imbalanceQuantityDirection'])
        
        # convert column format to numeric and fill with NaN if not numeric
        numeric_columns = df_merge[{'settlementPeriod','imbalancePriceAmountGBP','imbalanceQuantityMAW'}].columns
        df_merge[numeric_columns] = df_merge[numeric_columns].apply(pd.to_numeric, errors='coerce')
        
        # calculate absolute imbalance volumes
        df_merge['imbalanceQuantityMAW_absolute'] = abs(df_merge.imbalanceQuantityMAW)
        
        # calculate settlement period imbalance cost
        df_merge['cost'] = df_merge.imbalanceQuantityMAW_absolute * df_merge.imbalancePriceAmountGBP
        
         # calculate daily imbalance cost and unit rate
        df_cost_daily = df_merge.groupby(['settlementDate']).agg({'imbalanceQuantityMAW_absolute':'sum','cost':'sum'})
        
        df_cost_daily['unit_rate'] = df_cost_daily.cost / df_cost_daily.imbalanceQuantityMAW_absolute
        
        # output daily imbalance cost and unit rate
        print ('Total daily imbalance cost and unit rate:')
        print (df_cost_daily)
        
        # derived settlement hour from period
        df_merge['settlementHour'] = -(-df_merge.settlementPeriod//2)-1
        
        # calculate hourly imbalance volumes
        df_volume_hourly = df_merge.groupby(['settlementDate','settlementHour']).agg({'imbalanceQuantityMAW_absolute':'sum'})
        
        # output highest absolute imbalance volumes hour
        print()
        print ('Hour with highest absolute imbalance volumes:')
        print (df_volume_hourly.sort_values(by='imbalanceQuantityMAW_absolute',key=abs,ascending=False).head(1))
        
        # plot hourly absolute imbalance volumes
        df_volume_hourly.plot()
        
    except Exception as e:
        print (e)
        print ('calculation erroe')

