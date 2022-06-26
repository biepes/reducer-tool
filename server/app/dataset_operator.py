import csv
import os
import shutil
from datetime import datetime
from heapq import nlargest
from threading import Lock

import pandas as pd

from app.accumulator.priority_enum import Priority_enum
from app.config import config
from app.out_initializer import out_initializer


class Dataset_operator:

    lock_samples: Lock
    lock_sets: Lock

    accumulator_path: str

    size_of_sample: int
    size_of_sets: int
    size_of_groups: int

    has_final_counters: bool

    next_all_counters: bool

    def __init__(self):
        self.accumulator_path = f'{config.path}/out/accumulator'
        out_initializer.initialize()

        samples = self.read_samples()
        self.size_of_sample = samples.shape[0]

        self.size_of_sets = len([name for name in os.listdir(out_initializer.out_sets) if os.path.isfile(f'{out_initializer.out_sets}/{name}')])
        self.size_of_groups = len([name for name in os.listdir(out_initializer.out_groups) if os.path.isfile(f'{out_initializer.out_groups}/{name}')])
        
        self.next_all_counters = False

        self.lock_sets = Lock()
        self.lock_samples = Lock()

    def write_all_counters(self, counters):
        pd.DataFrame({'counters': counters}).to_csv(f'{self.accumulator_path}/all_counters.csv', index=False)

    def set_all_counters(self):
        counters = pd.read_csv(f'{self.accumulator_path}/all_counters.csv')
        self.write_representative_counters(list(counters['counters']))

    def read_samples(self) -> pd.DataFrame:
        try:
            return pd.read_csv(f'{self.accumulator_path}/samples.csv', low_memory=False)
        except FileNotFoundError:
            return pd.DataFrame()

    def write_samples(self, samples: pd.DataFrame):
        samples.to_csv(f'{self.accumulator_path}/samples.csv', index=False)
        self.size_of_sample = len(samples)

    def write_new_set(self, samples: pd.DataFrame):
        self.lock_sets.acquire()

        if len(samples) < config.max_samples:
            samples.to_csv(f'{out_initializer.out_sets}/{datetime.timestamp(datetime.now())}.csv', index=False)
            self.size_of_sets += 1
            self.write_samples(samples[0:0])

            if config.have_periodicity_all_counters:
                config.current_period = (config.current_period + 1) % config.periodicity_all_counters
                if config.current_period == 0:
                    self.next_all_counters = True
        else:
            list_of_dfs = [samples.loc[i:i+config.max_samples-1,:] for i in range(0, len(samples), config.max_samples)]

            for df in list_of_dfs:
                if len(df) == config.max_samples:
                    df.to_csv(f'{out_initializer.out_sets}/{datetime.timestamp(datetime.now())}.csv', index=False)
                    self.size_of_sets += 1
                    
                    if config.have_periodicity_all_counters:
                        config.current_period = (config.current_period + 1) % config.periodicity_all_counters
                        if config.current_period == 0:
                            self.next_all_counters = True
                else:
                    self.write_samples(df)

            if len(samples) % config.max_samples == 0:
                self.write_samples(samples[0:0])

        self.lock_sets.release()

    def load_sets(self) -> dict:
        list_of_sets = os.listdir(out_initializer.out_sets)

        datasets = dict()
        for set_name in list_of_sets:
            path = f'{out_initializer.out_sets}/{set_name}'
            datasets[set_name[:-4]] = pd.read_csv(path, low_memory=False)

        return datasets

    def write_representative_counters(self, counters):
        self.has_final_counters = True
        pd.DataFrame({'counters': counters}).to_csv(f'{config.path}/out/final_counters.csv', index=False)

    def read_representative_counters(self) -> pd.DataFrame:
        if (self.has_final_counters):
            return pd.read_csv(f'{config.path}/out/final_counters.csv')
        return None

    def rm_all_sets(self):
        list_of_sets = os.listdir(out_initializer.out_sets)
        for set_name in list_of_sets:
            path = f'{out_initializer.out_sets}/{set_name}'
            os.remove(path)
        
        self.size_of_sets = 0

    def rm_group(self, priority_enum: int):
        if (priority_enum == Priority_enum.TIME):
            self.rm_group_by_time()

    def rm_group_by_time(self):
        list_of_groups = os.listdir(out_initializer.out_groups)
        full_path = [f"{out_initializer.out_groups}/{groups_name}" for groups_name in list_of_groups]
        
        oldest_file = min(full_path, key=os.path.getctime)
        shutil.rmtree(oldest_file)

        self.size_of_groups -= 1

    def _n_newer_groups(self) -> list:
        if config.have_max_intersection_window:
            return nlargest(config.max_intersection_window, os.listdir(out_initializer.out_groups))
        
        list_of_groups = os.listdir(out_initializer.out_groups)
        list_of_groups.sort(key=lambda date: datetime.fromtimestamp(float(date[:-4])), reverse=True)

        return list_of_groups

    def write_dataset_by_name(self, dataset: pd.DataFrame, name: str):
        dataset.to_csv(name, index=False)

    def write_new_group(self, set_name: str, groups: list, distance: pd.DataFrame):
        path = f'{out_initializer.out_groups}/{set_name}'
        os.mkdir(path)

        with open(f'{path}/groups.csv', 'w', newline='') as file:
            csv.writer(file, delimiter=',').writerows(groups)

        distance.to_csv(f'{path}/distance.csv', index_label='index')

        self.size_of_groups += 1
        while (not config.devmode
                and config.have_max_intersection_window
                and self.size_of_groups > config.max_intersection_window):
            self.rm_group(config.remove_priority)

    def load_groups(self):
        list_of_groups = self._n_newer_groups()

        groups = list()
        distance_groups = list()
        for group_name in list_of_groups:
            path = f'{out_initializer.out_groups}/{group_name}'
            
            with open(f'{path}/groups.csv', newline='') as file:
                reader = csv.reader(file, delimiter=',')
                group_list = list(reader)
                group = [set(g) for g in group_list]
            
            distance = pd.read_csv(f'{path}/distance.csv', index_col='index', low_memory=False)

            groups.append(group)
            distance_groups.append(distance)

        return groups, distance_groups

ds_op = Dataset_operator()
