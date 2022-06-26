import numpy as np
import pandas as pd
from app.config import config
from app.data_process_analysis.preparation.statistics import \
    show_null_statistics


class Data_preparation:

    set: dict
    set_name: str

    def __init__(self, set: pd.DataFrame, set_name: str):
        self.set = set
        self.set_name = set_name
        pass

    def preprocessing_step(self) -> pd.DataFrame:
        set = self.nullable_elimination(self.set)
        set = set.astype('float')
        set = self.zero_variance_elimination(set)
        if (set.empty):
            return None

        return set
      
    def nullable_elimination(self, dataset: pd.DataFrame):
        dataset = dataset.replace(config.nullstr, np.nan)
        dataset_without_null = dataset.dropna(axis=1)
        
        if (config.devmode):
            show_null_statistics(dataset, dataset_without_null, self.set_name)


        return dataset_without_null

    def zero_variance_elimination(self, dataset: pd.DataFrame):
        variances = dataset.var(numeric_only=True)
        zero_variance_columns = variances[variances < 10e-30].index # Value because precision of algorithm
        copy_dataset = dataset.copy()
        copy_dataset.drop(zero_variance_columns, axis='columns', inplace=True)

        return copy_dataset
