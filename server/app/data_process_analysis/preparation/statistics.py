import pandas as pd
from app.config import config
from app.log_wrapper import log_wrapper


def show_null_statistics(dataset: pd.DataFrame, dataset_without_null: pd.DataFrame, dataset_name: str):
        null_statistics = pd.DataFrame()
        null_statistics['sum_null'] = dataset.isna().sum()
        null_statistics['all_null'] = dataset.dropna(axis=1, how='all').isna().sum()
        null_statistics['any_null'] = dataset.dropna(axis=1).isna().sum()
        null_statistics['per_null'] = null_statistics['sum_null']/dataset.shape[0]

        n_all_null = null_statistics['all_null'].isna().sum()
        n_any_null = null_statistics['any_null'].isna().sum()
        n_before = dataset.shape[1]
        n_after = dataset_without_null.shape[1]
        null_percentage = 100 - n_after*100/n_before

        percentages = [.1,.25,.5,.75]
        n_below_percentage = dict()
        for p in percentages:
            n_below_percentage[p] = len(null_statistics[(null_statistics['per_null'] <= p) & (null_statistics['per_null'] != 0)])

        log_wrapper.write_null_statistics(  dataset_name=dataset_name,
                                            cols_before=dataset.shape[1],
                                            cols_after=dataset_without_null.shape[1],
                                            perc_cols_any_null=null_percentage,
                                            n_all_null=n_all_null,
                                            n_any_null=n_any_null,
                                            n_below_percentage=n_below_percentage
                                        )
                                        