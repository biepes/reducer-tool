import configparser
from threading import Semaphore
import os

from app.accumulator.priority_enum import Priority_enum
from app.data_process_analysis.grouping.grouping_model import (
    Group_linkage_enum, Grouping_enum, Grouping_model)


class Config:

    path: str
    out_path: str
    workloads_path: str
    devmode: bool
    test: bool
    sem_test: Semaphore

    api_receiver: str
    min_samples: int
    max_samples: int
    periodic_time_samples: int
    max_intersection_window: int
    periodicity_all_counters: int
    remove_priority: int

    have_min_samples: bool
    have_max_intersection_window: bool
    have_periodicity_all_counters: bool

    nullstr: str
    method_grouping: int

    group_model: Grouping_model

    current_period: int

    def __init__(self):
        self.path = os.getcwd()

        config = configparser.ConfigParser()
        config.read(f'{self.path}/settings.conf')

        self.api_receiver = config['DEFAULT']['api_receiver']
        self.devmode = config['DEFAULT']['devmode'].lower() == 'true'
        self.test = config['DEFAULT']['test'].lower() == 'true'
        if self.test:
            self.sem_test = Semaphore(0)

        # Accumulator
        self.min_samples = int(config['ACCUMULATOR']['min_samples'])
        self.max_samples = int(config['ACCUMULATOR']['max_samples'])
        self.periodic_time_samples = int(config['ACCUMULATOR']['period_samples'])
        self.max_intersection_window = int(config['ACCUMULATOR']['max_intersection_window'])
        self.periodicity_all_counters = int(config['ACCUMULATOR']['periodicity_all_counters'])
        self.remove_priority = Priority_enum[config['ACCUMULATOR']['remove_priority']]
        
        self.have_min_samples = bool(self.min_samples)
        self.have_max_intersection_window = bool(self.max_intersection_window)
        self.have_periodicity_all_counters = bool(self.periodicity_all_counters)

        # Data process analysis
        self.nullstr = config['DATA_PROCESS_ANALYSIS']['nullstr'][1: -1]
        self.method_grouping = Grouping_enum[config['DATA_PROCESS_ANALYSIS']['method_grouping']]

        if self.method_grouping == Grouping_enum.PEARSON:
            group_linkage = Group_linkage_enum[config['GROUPING_PEARSON']['group_linkage']]
            cut_height = float(config['GROUPING_PEARSON']['cut_height'])

            self.group_model = Grouping_model(group_linkage, cut_height)
        
        self.out_path = f'{self.path}/out'
        self.workloads_path = f'{self.path}/workloads'

        self.current_period = 0

config = Config()
