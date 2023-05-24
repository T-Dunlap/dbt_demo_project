import pandas as pd
from prophet import Prophet

def model( dbt, session ):
    
    dbt.config(
        materialized="table",
        packages=['pandas','Prophet'] # how to import python libraries in dbt's context
    )

    # use historical data to fit model
    df = dbt.ref("agg_daily_returned_orders").to_pandas()
    df.columns = df.columns.str.lower()
    m = Prophet()
    m.fit(df)
    
    # forecast returns and output dataframe
    future = m.make_future_dataframe(periods=365)
    df = m.predict(future)
    
    return df
