import pandas as pd
from sklearn.model_selection import cross_val_score, KFold
from tensorflow import keras
from tensorflow.keras.wrappers.scikit_learn import KerasRegressor


def build_network(input_size: int):
    # create model
    model = keras.Sequential([
        # input layer
        keras.layers.Dense(
            input_size,
            input_shape=(input_size,),
            activation="sigmoid"
        ),
        keras.layers.Dense(64, activation="relu"),  # hidden layer
        keras.layers.Dense(1),  # output layer
    ])
    model.compile(optimizer="adam", loss="mean_squared_error")
    print(model.summary())
    return model


def main():
    # load and clean data
    data = pd.read_csv("data/cleaned_data.csv")

    # select target and feature columns
    y = data["sell_factor"]
    x = data.drop("sell_factor", axis=1)

    estimator = KerasRegressor(
        build_fn=lambda: build_network(x.shape[1]),
        epochs=20,
        batch_size=5,
        verbose=1
    )
    kfold = KFold(n_splits=10, shuffle=True)
    results = cross_val_score(estimator, x, y, scoring="r2", n_jobs=-1, cv=kfold)
    print(f"r^2 Score: {results.mean()}")


if __name__ == "__main__":
    main()
