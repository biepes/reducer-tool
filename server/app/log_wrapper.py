from datetime import datetime
from tabulate import tabulate
from app.config import config
from app.out_initializer import out_initializer


class Log_wrapper:

    log_file: str
    null_statistics_file: str
    counter_statistics_file: str

    headers_counter_statistics: list
    counter_statistics_table: list

    def __init__(self) -> None:
        out_initializer.initialize()
        self.log_file = f'{config.path}/out/accumulator.log'
        file = open(self.log_file, 'w')
        file.close()
        
        self.null_statistics_file = f'{config.path}/out/statistics/null_statistics.log'
        file = open(self.null_statistics_file, 'w')
        file.close()

        self.counter_statistics_file = f'{config.path}/out/statistics/counter_statistics.log'
        self.headers_counter_statistics = list(['time', 'set_name', 'counters', 'counters_after_preparation', 'counters_after_grouping', 'intersection'])
        self.counter_statistics_table = list()

    def write(self, log: str):
        all_log = f'[{datetime.now()}] {log}'

        file = open(self.log_file, 'a')
        file.write(f'{all_log}\n')
        file.close()

    def write_null_statistics(self, dataset_name: str, cols_before: int, cols_after: int, perc_cols_any_null: float, n_all_null: int, n_any_null: int, n_below_percentage: dict):
        out = f'----------\t\t{datetime.now()}\t\t----------\n'
        out += f'dataset: {dataset_name}\n'
        out += f'Number of columns before null elimination: {cols_before}\n'
        out += f'Number of columns after null elimination: {cols_after}\n'
        out += f'Percentage of column with any value null: {perc_cols_any_null:.2f}%\n'
        out += f'Number of removed columns if all values are null: {n_all_null}\n'
        out += f'Number of removed columns if any value are null: {n_any_null}\n'
        out += f'Number of removed columns where any value are null but not all: {n_any_null - n_all_null}\n'
        for p in n_below_percentage.keys().__reversed__():
            out += f'Number of removed columns if percent of null is below to {p*100:.0f}%: {n_below_percentage[p]}\n'

        file = open(self.null_statistics_file, 'a')
        file.write(out)
        file.close()

    def write_counter_statistics(self, set_name: str, counters_len: int, counters_after_preparation: int, counters_after_grouping: int = None):
        self.counter_statistics_table.append([f'[{datetime.now()}]', f'C_TIME_{set_name}', counters_len, counters_after_preparation, counters_after_grouping, '-'])
       
        file = open(self.counter_statistics_file, 'w')
        file.write(tabulate(self.counter_statistics_table, headers=self.headers_counter_statistics))
        file.close()

    def write_intersection_statistics(self, counters_len: int, sets_len: int):
        self.counter_statistics_table.append([f'[{datetime.now()}]', '-', '-', '-', '-', f'n groups: {sets_len}; after intersection: {counters_len}'])

        file = open(self.counter_statistics_file, 'w')
        file.write(tabulate(self.counter_statistics_table, headers=self.headers_counter_statistics))
        file.close()

log_wrapper = Log_wrapper()
