"""Clean the merged data and prepare it for training."""

import pandas as pd
from sklearn.preprocessing import StandardScaler

DROP_FEATURES = ["system_name", "station", "market_id", "timestamp", "buy_price", "name", "sell_price", "mean_price"]

CATEGORICAL_FEATURES = ["station_allegiance", "station_government", "station_type", "faction_state", "security"]

SCALE_FEATURES = [
    "demand", "demand_bracket", "stock", "stock_bracket", "population", "stars", "metal_bodies", "rock_bodies",
    "ice_bodies", "water_bodies", "gas_giants", "rocky_rings", "icy_rings", "metal_rich_rings", "metallic_rings"
]


def clean():
    """Clean the data to prepare it for training."""
    data = pd.read_csv("data/merged_data.csv")

    # Calculate sell price factor.
    data["sell_factor"] = data["sell_price"] / data["mean_price"]

    # Drop ID features and other unneeded features.
    data = data.drop(columns=DROP_FEATURES)

    # One-hot encode categorical features.
    data = pd.get_dummies(data, columns=CATEGORICAL_FEATURES)

    # Scale continuous features with a huge range.
    scaled_features = pd.DataFrame(
        StandardScaler().fit_transform(data[SCALE_FEATURES]),
        columns=SCALE_FEATURES,
        index=data.index
    )
    data[SCALE_FEATURES] = scaled_features

    # Fill NaN values
    data = data.fillna(0)

    data.to_csv("data/cleaned_data.csv", index=False)


if __name__ == "__main__":
    clean()
