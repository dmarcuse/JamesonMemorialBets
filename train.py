import tensorflow as tf
from tensorflow import keras

import pandas as pd

from sklearn.model_selection import train_test_split

from clean import clean

# how many epochs to train
num_epochs = 20

# how much of the training data to use for validation (0.2 = 20%)
validation_split = 0.2


def evaluate(target, predictions):
    # TODO
    pass


def main():
    # load and clean data
    data = clean(pd.read_csv("data/merged_data.csv"))

    # select target and feature columns
    y = data["sell_factor"]
    x = data.drop("sell_factor", axis=1)

    # split into training and testing set
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=validation_split)

    # create model
    model = keras.Sequential([
        # input layer
        keras.layers.Dense(
            x_train.shape[1],
            input_shape=(x_train.shape[1],),
            activation="sigmoid"
        ),
        keras.layers.Dense(64, activation="relu"),  # hidden layer
        keras.layers.Dense(1, activation="relu"),   # output layer
    ])
    model.compile(optimizer="sgd", loss="mean_squared_error")
    print(model.summary())

    # train model
    model.fit(
        x_train,
        y_train,
        epochs=num_epochs,
        verbose=1,
        validation_data=(x_test, y_test)
    )

    # make predictions
    print("TRAINING SET:")
    evaluate(y_train, model.predict(x_train))
    print("VALIDATION SET:")
    evaluate(y_test, model.predict(y_test))


if __name__ == "__main__":
    main()
