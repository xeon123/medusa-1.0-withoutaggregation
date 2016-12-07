from dogpile.cache import make_region

# my_dictionary = {}
region = make_region().configure('dogpile.cache.dbm',
                                 expiration_time=7200,
                                 arguments={
                                     "filename": "./cache_execution.dbm"
                                 })

# region = make_region().configure('dogpile.cache.memory')


# arguments={"cache_dict":my_dictionary})
class SimpleCache:
    @staticmethod
    def clean():
        """
        clean file saved in cache
        """
        SimpleCache.delete("extra_cluster")
        SimpleCache.delete("mapreduce.input.path")
        SimpleCache.delete("pick_up_clusters")
        SimpleCache.delete("reduce_input_path")
        SimpleCache.delete("clusters")
        SimpleCache.delete("job.step.execution")
        SimpleCache.delete("fault_injector")
        SimpleCache.delete("job")


    @staticmethod
    def save(key, value):
        """
        general purpose method to save data (value) in the cache

        :param key (string) key of the value to be saved in cache
        :param value (any type) the value to be saved
        """
        region.set(key, value)


    @staticmethod
    def get(key):
        """
        general purpose method to get data from the cache

        :param key (string) key of the data to be fetched
        :return value (any type) data to be returned from the cache
        """
        return region.get(key)


    @staticmethod
    def get_or_create(key):
        """
        General purpose method to get data from the cache. If the value does not exist, it creates a list

        :param: key (string) key of the data to be fetched
        :return value (any type) data to be returned from the cache
        """
        return region.get_or_create(key, list)


    @staticmethod
    def delete(key):
        """
        General purpose method to get data from the cache. If the value does not exist, it creates a list

        :param: key (string) key of the data to be fetched
        """
        region.delete(key)


    @staticmethod
    def set_running_clusters_cache(clusters):
        """ set running clusters in cache """
        SimpleCache.save("clusters", clusters)


    @staticmethod
    def get_running_clusters_cache():
        """ get running clusters that are save in cache """
        return SimpleCache.get("clusters")


    @staticmethod
    def set_pick_up_clusters(clusters):
        """ set picked up clusters in cache """
        SimpleCache.save("pick_up_clusters", clusters)


    @staticmethod
    def get_pick_up_clusters():
        """ get picked up clusters from the cache """
        return SimpleCache.get_or_create("pick_up_clusters")


    @staticmethod
    def get_extra_cluster():
        """ get running clusters that are save in cache """
        return SimpleCache.get("extra_cluster")


    @staticmethod
    def set_extra_cluster(cluster):
        """ set picked up clusters in cache """
        SimpleCache.save("extra_cluster", cluster)


    @staticmethod
    def get_runtime_output_files_cache(key):
        """
        get the list of files that are in the cache
        :param key: (string) id of the job execution
        :return: (list) list of files
        """
        return SimpleCache.get(key)


    @staticmethod
    def set_reduce_input_path(input_path):
        """ Save data in the cache. This is a general method.

        :param input_path (list) reduce input path
        """
        SimpleCache.save("reduce_input_path", input_path)


    @staticmethod
    def append_reduce_input_path(input_path):
        """
        Append input path in the cache
        :param input_path:
        :return:
        """

        old_input_path = SimpleCache.get_or_create("reduce_input_path")
        old_input_path += input_path

        SimpleCache.save("reduce_input_path", old_input_path)


    @staticmethod
    def get_from_list(key):
        """
        Get the values from the list. This is a general method.
        :param key: (string) to obtain the value
        :return value (list) associated to the key
        """
        return SimpleCache.get_or_create(key)


    @staticmethod
    def set_step_execution(step):
        SimpleCache.save("job.step.execution", step)


    @staticmethod
    def get_step_execution():
        return SimpleCache.get("job.step.execution")


    @staticmethod
    def set_input_path(input_path):
        SimpleCache.save("mapreduce.input.path", input_path)


    @staticmethod
    def get_input_path():
        return SimpleCache.get("mapreduce.input.path")


    @staticmethod
    def set_fault_injector(injector):
        SimpleCache.save("fault_injector", injector)


    @staticmethod
    def get_fault_injector():
        return SimpleCache.get("fault_injector")


class MemoizeCalls:
    """ This class saves the remove function calls. """
    def __init__(self, path, queue, func):
        self.path = path
        self.queue = queue
        self.func = func
