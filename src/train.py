import datasets
from models import ElasticNetModel, NNModel

def train(args):
    test, train, val = datasets.get_partitions(args)
    model_type = ElasticNetModel if args.model_type == 'elastic' else NNModel
    model = model_type(args)

    # insert other definitions here, e.g. callbacks

    # calls the fit function for the model class; actual calls to TF and sklearn happen in the class function
    model.fit(data=train, val_data=val) # other parameters as well

    model.evaluate(data=test)