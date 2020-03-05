"""Clean the merged data and prepare it for training."""

import pandas as pd

DROP_FEATURES = ["system_name", "station", "market_id", "timestamp", "buy_price", "name", "sell_price", "mean_price"]

CATEGORICAL_FEATURES = ["station_allegiance", "station_government", "station_type", "faction_state", "security"]


def clean(data: pd.DataFrame) -> pd.DataFrame:
    """Clean the data to prepare it for training."""
    data = pd.read_csv("data/merged_data.csv")

    # Calculate sell price factor.
    data["sell_factor"] = data["sell_price"] / data["mean_price"]

    # Drop ID features and other unneeded features.
    data = data.drop(columns=DROP_FEATURES)

    # One-hot encode categorical features.
    data = pd.get_dummies(data, columns=CATEGORICAL_FEATURES)

    # Fill NaN values
    data = data.fillna(0)

    return data
