import pandas as pd
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from tensorflow import keras

# How many epochs to train.
NUM_EPOCHS = 20

# How much of the training data to use for testing (0.2 = 20%).
VALIDATION_SPLIT = 0.2

# The name of the target feature.
TARGET_FEATURE = "sell_difference"


def main():
    # Load and clean data.
    data = pd.read_csv("data/cleaned_data.csv")

    # Select target and feature columns.
    y = data[TARGET_FEATURE]
    x = data.drop(TARGET_FEATURE, axis=1)

    # Split into training and testing set.
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=VALIDATION_SPLIT)

    # Create the model.
    model = keras.Sequential([
        keras.layers.Dense(x_train.shape[1], input_shape=(x_train.shape[1],)),  # Input layer
        keras.layers.Dense(64, activation="relu"),  # Hidden layer
        keras.layers.Dense(64, activation="relu"),  # Hidden layer
        keras.layers.Dense(1),  # Output layer
    ])
    model.compile(optimizer="adam", loss="mean_squared_error")
    print(model.summary())

    # Train the model.
    model.fit(
        x_train,
        y_train,
        epochs=NUM_EPOCHS,
        verbose=1
    )

    # Score the model.
    print(f"r2 score (training): {r2_score(y_train, model.predict(x_train))}")
    print(f"r2 score (testing): {r2_score(y_test, model.predict(x_test))}")


if __name__ == "__main__":
    main()
