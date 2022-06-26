import os
import shutil


class Out_initializer():

    out_path: str
    out_groups: str
    out_sets: str

    def __init__(self) -> None:
        self.out_path = f'{os.getcwd()}/out'
        self.out_groups = f'{self.out_path}/accumulator/groups'
        self.out_sets = f'{self.out_path}/accumulator/sets'

    def initialize(self):
        if os.path.exists(self.out_path):
            shutil.rmtree(self.out_path)
        os.mkdir(f'{self.out_path}')
        os.mkdir(f'{self.out_path}/accumulator')
        os.mkdir(self.out_sets)
        os.mkdir(self.out_groups)
        os.mkdir(f'{self.out_path}/statistics')

out_initializer = Out_initializer()
