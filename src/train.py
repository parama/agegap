import datasets
from models import ElasticNetModel, NNModel
import yaml
import sys

def get_args(config):
    with open(config, 'rb') as f:
        args = yaml.load(f.read())
    return args

def train(args):

    test, train, val = datasets.get_partitions(args)
    model = NNModel(args)

    # insert other definitions here, e.g. callbacks

    # calls the fit function for the model class; actual calls to TF and sklearn happen in the class function
    model.fit(data=train, val_data=val) # other parameters as well

    model.evaluate(data=test)

if __name__=='__main__':
    config_file = sys.argv[1]
    args = get_args(config_file)
    train(args)