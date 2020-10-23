import tensorflow as tf

def get_partitions(args):
    '''
    Partitions the data indices in args.data_indices into test, train, and validation data.
    Returns (test, train, val), a tuple of CustomDataset objects.
    '''
    pass


class CustomDataset:
    def __init__(self, indices, dataset_type, buffer_size):
        self.indices = indices
        self.dataset_type = dataset_type
        self.dataset = tf.data.Dataset.from_tensor_slices(indices)
        self.dataset = self.dataset.shuffle(buffer_size, reshuffule_each_iteration=True)
        self.dataset = self.dataset.map(lambda x: tf.py_function(func=load_data, inp=[x], Tout=tf.float32))

    def load_data(self, index):
        '''
        Queries the database server to extract the data for one index.
        '''
        pass
