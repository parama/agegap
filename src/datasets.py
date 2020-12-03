import tensorflow as tf
import preprocessing
import numpy as np
import math

def get_partitions(data_indices, database, num_test, num_val):
    '''
    Partitions data_indices of database into test, train, and validation data.
    Returns (test, train, val), a tuple of CustomDataset objects.
    '''
    shuffled_indices = np.random.shuffle(data_indices)
    val_idx, test_idx, train_idx = shuffled_indices[:num_val], \
                                   shuffled_indices[num_val:num_val+num_test], \
                                   shuffled_indices[num_val+num_test:]

    test = CustomDataset(test_idx, "test", database)
    train = CustomDataset(train_idx, "train", database)
    val = CustomDataset(val_idx, "val", database)

    return test, train, val


class CustomDataset:
    def __init__(self, indices, dataset_type, database, args):
        self.indices = indices
        self.dataset_type = dataset_type
        reshuffle_each_iteration = True if self.dataset_type == "train" else False
        self.dataset = tf.data.Dataset.from_tensor_slices(indices)
        self.dataset = self.dataset.shuffle(args['buffer_size'], reshuffule_each_iteration=reshuffle_each_iteration)
        self.dataset = self.dataset.map(lambda x: tf.py_function(func=load_data, inp=[x], Tout=tf.float32))
        self.dataset = self.dataset.batch(args['batch_size'], drop_remainder=True)
        if not dataset_type == 'test':
            self.dataset = self.dataset.repeat()
        self.database = database
        self.num_steps = math.floor(len(indices)/args['batch_size'])

    def load_data(self, index):
        '''
        Queries the database server to extract the data for one index.
        '''
        return preprocessing.get_data(self.database, index)
