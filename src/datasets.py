import tensorflow as tf
import preprocessing
import database
import numpy as np
import math

def get_partitions(args, num_test, num_val):
    '''
    Partitions data_indices of database into test, train, and validation data.
    Returns (test, train, val), a tuple of CustomDataset objects.
    '''
    data_indices = database.load_indices(args['data_table'])
    num_test, num_val = args['num_test'], args['num_val']

    shuffled_indices = np.random.shuffle(data_indices)
    val_idx, test_idx, train_idx = shuffled_indices[:num_val], \
                                   shuffled_indices[num_val:num_val+num_test], \
                                   shuffled_indices[num_val+num_test:]

    test = CustomDataset(test_idx, "test", args)
    train = CustomDataset(train_idx, "train", args)
    val = CustomDataset(val_idx, "val", args)

    return test, train, val


class CustomDataset:
    def __init__(self, indices, dataset_type, args):
        self.indices = indices
        self.dataset_type = dataset_type
        reshuffle_each_iteration = True if self.dataset_type == "train" else False
        self.dataset = tf.data.Dataset.from_tensor_slices(indices)
        self.dataset = self.dataset.shuffle(args['buffer_size'], reshuffule_each_iteration=reshuffle_each_iteration)
        self.dataset = self.dataset.map(lambda x: tf.py_function(func=load_data, inp=[x], Tout=tf.float32))
        self.dataset = self.dataset.batch(args['batch_size'], drop_remainder=True)
        if not dataset_type == 'test':
            self.dataset = self.dataset.repeat()
        self.database = args["database"]
        self.table = args["data_table"]
        self.num_steps = math.floor(len(indices)/args['batch_size'])

    def load_data(self, index):
        '''
        Queries the database server to extract the data for one index.
        '''
        return database.get_data(self.database, self.table, index)
