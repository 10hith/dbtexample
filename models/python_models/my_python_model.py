def model(dbt, session):
    import pandas as pd

    dbt.config(
        materialized="table",
        packages=["pandas"]
    )
    df = pd.DataFrame({'num_legs': [2, 4, 8, 0],
                       'num_wings': [2, 0, 0, 0],
                       'num_specimen_seen': [10, 2, 1, 8]},
                      index=['falcon', 'dog', 'spider', 'fish'])
    return df