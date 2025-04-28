import pandas as pd
import numpy as np

df = pd.DataFrame({
    'customer_id': np.arange(1000),
    'signup_date': pd.date_range(start='2022-01-01', periods=1000),
    'country': np.random.choice(['US', 'UK', 'IN'], size=1000)
})

df.to_csv("synthetic_customers.csv", index=False)