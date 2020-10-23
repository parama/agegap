import tensorflow as tf                     # for NNModel
from sklearn.linear_model import ElasticNet # for ElasticNetModel

class NNModel:
    def __init__(self, args):
        self.args = args
        self.model = None

    def fit(self, data, val_data):
        pass

    def evaluate(self, data):
        pass

class ElasticNetModel:
    def __init__(self, args):
        self.args = args
        self.model = None

    def fit(self, data, val_data):
        pass

    def evaluate(self, data):
        pass