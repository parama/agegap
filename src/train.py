import datasets
from models import ElasticNetModel, NNModel
import yaml
import sys

def get_args(config):
    with open(config, 'rb') as f:
        args = yaml.load(f.read())
    return args

def train(args):

    # general model
    full_dataset = args['dataset']
    full_data_indices = load_indices(args['data_indices'])

    test, train, val = datasets.get_partitions(full_data_indices, full_dataset, args['num_test'], args['num_val'])
    model = NNModel(args)

    # insert other definitions here, e.g. callbacks

    # calls the fit function for the model class; actual calls to TF and sklearn happen in the class function
    model.fit(data=train, val_data=val) # other parameters as well

    model.evaluate(data=test)

if __name__=='__main__':
    config_file = sys.argv[1]
    args = get_args(config_file)
    train(args)