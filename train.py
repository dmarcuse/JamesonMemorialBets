import pandas as pd
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split
from tensorflow import keras
from os import path
from random import random

# How many epochs to train.
NUM_EPOCHS = 20

# How much of the training data to use for testing (0.2 = 20%).
VALIDATION_SPLIT = 0.2

# The name of the target feature.
TARGET_FEATURE = "sell_factor"

# Path to save/load model from.
MODEL_FILE = "bets.tf"


def train_model(x_train, y_train, x_test, y_test):
    # Create the model.
    model = keras.Sequential([
        keras.layers.Dense(x_train.shape[1], input_shape=(x_train.shape[1],)),  # Input layer
        keras.layers.Dense(32, activation="relu"),  # Hidden layer
        keras.layers.Dense(16, activation="relu"),  # Hidden layer
        keras.layers.Dense(8, activation="relu"),  # Hidden layer
        keras.layers.Dense(1),  # Output layer
    ])
    model.compile(optimizer="adam", loss="mean_squared_error")
    print(model.summary())

    # Train the model.
    model.fit(
        x_train,
        y_train,
        epochs=NUM_EPOCHS,
        verbose=1,
        validation_data=(x_test, y_test)
    )

    return model


def evaluate(expected, predictions, name):
    print(name + ":")
    print(f"\tr² score: {r2_score(expected, predictions)}")
    print(f"\tmean absolute error: {mean_absolute_error(expected, predictions)}")


def main():
    # Load and clean data.
    data = pd.read_csv("data/cleaned_data.csv")

    # Select target and feature columns.
    y = data[TARGET_FEATURE]
    x = data.drop(TARGET_FEATURE, axis=1)

    # Split into training and testing set.
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=VALIDATION_SPLIT)

    # Load or train model
    if path.exists(MODEL_FILE):
        model = keras.models.load_model(MODEL_FILE)
        print(model.summary())
    else:
        model = train_model(x_train, y_train, x_test, y_test)
        keras.models.save_model(model, MODEL_FILE)

    # Score the model.
    evaluate(y_train, model.predict(x_train), "training")
    evaluate(y_test, model.predict(x_test), "testing")
    evaluate(y, [random() * 5 for _ in y], "random")


if __name__ == "__main__":
    main()
