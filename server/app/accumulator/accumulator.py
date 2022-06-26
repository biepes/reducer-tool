import time
from io import StringIO
from threading import Lock, Thread

import pandas as pd
from app.config import config
from app.data_process_analysis.analysis import Data_process_analysis
from app.dataset_operator import ds_op
from app.log_wrapper import log_wrapper
from flask import Flask, Response, make_response, request

last_size_sets: int

lock_acumm: Lock

class _EndpointAction:

    def __init__(self, action, headers = {}):
        self.action = action
        self.response = Response(status=200, headers=headers)

    def __call__(self):
        return self.action()

class _FlaskAppWrapper:
    app = None

    def __init__(self, name):
        self.app = Flask(name)

    def run(self):
        self.app.run(host='0.0.0.0')

    def add_endpoint(self, endpoint=None, endpoint_name=None, methods=None, handler=None, headers=None):
        self.app.add_url_rule(endpoint, endpoint_name, _EndpointAction(handler, headers), methods=methods)

class Accumulator_time_wrapper(Thread):

    def __init__(self):
        log_wrapper.write(f'* Accumulator\t|\tperiodic init')

        Thread.__init__(self)

    def run(self):
        while(True):
            time.sleep(config.periodic_time_samples)
            log_wrapper.write(f'* Accumulator\t|\tperiodic execute')

            if (ds_op.size_of_sample >= config.min_samples):
                ds_op.lock_samples.acquire()
                samples = ds_op.read_samples()
                ds_op.lock_samples.release()

                ds_op.write_new_set(samples)
                log_wrapper.write(f'* Accumulator\t|\tperiodic new set')

            if ds_op.size_of_sets > 0:
                Analysis_wrapper().start()

class Analysis_wrapper(Thread):

    def __init__(self):
        Thread.__init__(self)

    def run(self):
        lock_acumm.acquire()
        ds_op.lock_sets.acquire()
        try:
            Data_process_analysis().execute()
        except Exception as e:
            log_wrapper.write(e)

        ds_op.lock_sets.release()
        lock_acumm.release()


class Accumulator:
    
    app: _FlaskAppWrapper
    headers: str
    first_set: bool

    def __init__(self):
        global lock_acumm, last_size_sets
        lock_acumm = Lock()
        
        self.first_set = True
        last_size_sets = ds_op.size_of_sets
        if (last_size_sets > 0):
            anw = Analysis_wrapper()
            anw.start()

        if (config.have_min_samples):
            atw = Accumulator_time_wrapper()
            atw.start()

        # Api
        self.app = _FlaskAppWrapper('wrap')
        self.app.add_endpoint(endpoint='/insert_sample', endpoint_name='insert sample', methods=['POST'], handler=self.insert_sample)
        self.app.add_endpoint(endpoint='/read_representative_counters', endpoint_name='read representative counters', methods=['GET'], handler=self.read_representative_counters)

        self.app.run()

        # atw.join()

    def insert_sample(self):
        file_content = request.files['sample']
        if file_content:
            byte_content = file_content.read()
            if len(byte_content) == 0:
                return {'file': 'OK'}
            content = byte_content.decode()
            file = StringIO(content)

            ds_op.lock_samples.acquire()
            samples = ds_op.read_samples()
            csv_data = pd.read_csv(file, low_memory=False)
            samples = pd.concat([samples, csv_data], ignore_index=True)

            if config.have_periodicity_all_counters and self.first_set:
                ds_op.write_all_counters(samples.columns)
                self.first_set = False

            ds_op.write_samples(samples)
            log_wrapper.write(f'* Accumulator\t|\tinserted {csv_data.shape[0]} samples with {csv_data.shape[1]} counters')

            size = len(samples)
            if(size >= config.max_samples):
                lock_acumm.acquire()
                ds_op.write_new_set(samples)
                lock_acumm.release()
                log_wrapper.write(f'* Accumulator\t|\t{ds_op.size_of_sets} new sets')
                if config.have_periodicity_all_counters and ds_op.next_all_counters:
                    log_wrapper.write(f'* Accumulator\t|\treturn all counters')
                    ds_op.set_all_counters()
                    ds_op.next_all_counters = False
                    if config.test:
                        config.sem_test.release()
                else:
                    Analysis_wrapper().start()
            else:
                if config.test:
                    config.sem_test.release()
            ds_op.lock_samples.release()

        if config.test:
            config.sem_test.acquire()

        return {'file': 'OK'}

    def read_representative_counters(self):
        lock_acumm.acquire()
        
        output = make_response(ds_op.read_representative_counters().to_csv(index=False))

        output.headers["Content-Disposition"] = "attachment; filename=export.csv"
        output.headers["Content-type"] = "text/csv"
        lock_acumm.release()

        return output


if __name__ == '__main__':
    Accumulator()
    