class FileRead:
    def __init__(self, spark_context=None):
        self.spark_context = spark_context

    def read_data_from_file(self, file_path, file_names, file_type):
        dataframes_dict = {}
        for item in file_names:
            file_name = file_path + '\\' + item + '.' + file_type
            df_read = self.spark_context.read.load(file_name, file_type, header="true", inferSchema="true",
                                                   multiLine="true")
            dataframes_dict[item] = df_read
        return dataframes_dict
