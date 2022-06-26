import numpy as np
import pandas as pd
from app.config import config
from app.data_process_analysis.grouping.grouping_model import Grouping_enum
from sklearn import cluster as skcluster


class Grouping:

    dataset: pd.DataFrame

    def __init__(self, dataset: dict):
        self.dataset = dataset

    def execute_group(self):
        if config.method_grouping == Grouping_enum.PEARSON:
            return self.__grouping_pearson()

    def __grouping_pearson(self):
        distance_matrix = self.__pearson_dissimilarity_matrix(self.dataset)
        g_s = self.__complete_linkage_cluster(distance_matrix)

        return g_s, distance_matrix
        
    def __d_coeficient(self, r: float):
        if r >= 0:
            return 1-r
        else:
            return r+1

    def __pearson_dissimilarity_matrix(self, dataset: pd.DataFrame):
        diss_matrix = dataset.corr(method="pearson")
        distance_matrix = diss_matrix.fillna(0)
        distance_matrix = distance_matrix.applymap(self.__d_coeficient)

        return distance_matrix

    def __complete_linkage_cluster(self, distance_matrix: pd.DataFrame):
        clusters = skcluster.AgglomerativeClustering(linkage="complete",
                                                    affinity="precomputed", 
                                                    n_clusters=None, 
                                                    distance_threshold=config.group_model.cut_height).fit_predict(distance_matrix)
        
        name_counters = distance_matrix.columns
        groups = np.empty((clusters.max()+1,), object)
        groups[...]=[set([]) for _ in range(clusters.max()+1)]
        for idx, group in enumerate(clusters):
            groups[group].add(name_counters[idx])

        return groups
