In [1]: import pandas as pd

In [2]: import numpy as np

In [3]: import matplotlib.pyplot as plt

In [4]: df1 = pd.read_csv('page_per_day_hits_tmp.csv', names=['day', 'month', 'year', 'page', 'daily'], header=0)
Out[4]:
     day  month  year                page  daily
0      3      2  2014             cart.do    115
1      4      2  2014             cart.do    681
..   ...    ...   ...                 ...    ...
103   10      2  2014      stuff/logo.ico      3

[104 rows x 5 columns]

In [5]: grouped = df1.groupby(['page'])
Out[5]: <pandas.core.groupby.DataFrameGroupBy object at 0x10f6b0dd0>

In [6]: grouped.agg({'daily':'sum'}).plot(kind='bar')
Out[6]: <matplotlib.axes.AxesSubplot at 0x10f8f4d10>

['State', 'Account Length', 'Area Code', 'Phone', "Int'l Plan", 'VMail Plan', 'VMail Message', 'Day Mins', 'Day Calls', 'Day Charge', 'Eve Mins', 'Eve Calls', 'Eve Charge', 'Night Mins', 'Night Calls', 'Night Charge', 'Intl Mins', 'Intl Calls', 'Intl Charge', 'CustServ Calls', 'Churn?']

  State  Account Length  Area Code     Phone Intl Plan VMail Plan  \
0    KS             128        415  382-4657         no        yes
1    OH             107        415  371-7191         no        yes
2    NJ             137        415  358-1921         no         no
3    OH              84        408  375-9999        yes         no

   Night Charge  Intl Mins  Intl Calls  Intl Charge  CustServ Calls  Churn?
0         11.01       10.0           3         2.70               1  False.
1         11.45       13.7           3         3.70               1  False.
2          7.32       12.2           5         3.29               0  False.
3          8.86        6.6           7         1.78               2  False.

confusion_matrices = [
    ( "Support Vector Machines", confusion_matrix(y,run_cv(X,y,SVC)) ),
    ( "Random Forest", confusion_matrix(y,run_cv(X,y,RF)) )
]
