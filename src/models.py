import tensorflow as tf                     # for NNModel
from tensorflow import keras
from tensorflow.keras import layers
from sklearn.linear_model import ElasticNet # for ElasticNetModel
import matplotlib.pyplot as plt

class NNModel:
    def __init__(self, args):
        self.args = args
        self.model = self.basic()

    def basic(self):
        print("Build model")
        model = keras.models.Sequential()
        model.add(layers.Dense(200, input_dim=self.args["num_genes"], name="Dense1"))
        model.add(layers.BatchNormalization())
        model.add(keras.layers.Activation('relu'))
        model.add(keras.layers.Dropout(0.25))
        model.add(keras.layers.Dense(200, name="Dense2"))
        model.add(keras.layers.BatchNormalization())
        model.add(keras.layers.Activation('relu'))
        model.add(keras.layers.Dropout(0.25))
        model.add(keras.layers.Dense(1, name="Dense3"))
        model.add(keras.layers.BatchNormalization())
        model.add(keras.layers.Activation('linear'))

        model.summary()
        return model

    def fit(self, data, val_data):
        model = self.model
        model.compile(loss='mse', optimizer='adam', metrics=['mse', 'mae'])
        es = keras.callbacks.EarlyStopping(monitor='val_loss', verbose=1)
        checkpoint_filepath = 'tmp/checkpoint'
        mc = keras.callbacks.ModelCheckpoint(checkpoint_filepath, save_best_only=True)
        print("starting to train...")
        history = model.fit(x=data.dataset, epochs=self.args['epochs'], verbose=1, 
                            callbacks=[es, mc], validation_data=val_data.dataset, 
                            shuffle=False, steps_per_epoch=data.num_steps, validation_steps=val_data.num_steps)
        print("done training")
        model.load_weights(checkpoint_filepath)
        model.save(args['output_model_path'])

        print(history.history.keys())
        # "Loss"
        plt.plot(history.history['loss'])
        plt.plot(history.history['val_loss'])
        plt.title('model loss')
        plt.ylabel('loss')
        plt.xlabel('epoch')
        plt.legend(['train', 'validation'], loc='upper left')
        plt.show()

        self.model = model

    def evaluate(self, data):
        model = self.model
        results = model.evaluate(x=data.dataset)
        print(model.metrics_names)
        print(results)
