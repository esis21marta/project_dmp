# Packages
import pandas as pd


# Define function
def Process_Security_ACLED(
    Security_ACLED_df: pd.core.frame.DataFrame,
) -> pd.core.frame.DataFrame:

    # Process data
    Security_ACLED_df = Security_ACLED_df.loc[
        Security_ACLED_df["COUNTRY"] == "Indonesia"
    ]
    Security_ACLED_df = Security_ACLED_df[
        ["SUB_EVENT_TYPE", "EVENT_TYPE", "LATITUDE", "LONGITUDE"]
    ]

    # Return
    return Security_ACLED_df
