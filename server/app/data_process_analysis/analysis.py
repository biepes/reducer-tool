from app.config import config
from app.data_process_analysis.grouping.grouping import Grouping
from app.data_process_analysis.preparation.data_preparation import \
    Data_preparation
from app.data_process_analysis.representative_counters.rp_count import \
    Representative_counters
from app.dataset_operator import ds_op
from app.log_wrapper import log_wrapper


class Data_process_analysis:

    def execute(self):        
        sets = ds_op.load_sets()
        log_wrapper.write(f'* Analysis\t\t|\tstart with {len(sets)} sets')

        for set_name in sets.keys():
            set = sets[set_name]
            set_after_preparation = Data_preparation(set, set_name).preprocessing_step()
            if (set_after_preparation is None):
                log_wrapper.write_counter_statistics(set_name, set.shape[1], 0)
                continue

            groups, distance = Grouping(set_after_preparation).execute_group()
            ds_op.write_new_group(set_name, groups, distance)

            log_wrapper.write_counter_statistics(set_name, set.shape[1], set_after_preparation.shape[1], len(groups))

        ds_op.rm_all_sets()

        groups, distance_groups = ds_op.load_groups()
        if (len(groups) < 2):
            counters  = Representative_counters().get_representative_counters(groups[0], distance_groups)
            ds_op.write_representative_counters(counters)

            if config.test:
                config.sem_test.release()

            log_wrapper.write(f'* Analysis\t\t|\tend with {len(counters)} representative counters')
            return

        counters  = Representative_counters(groups, distance_groups).execute()
        ds_op.write_representative_counters(counters)

        log_wrapper.write_intersection_statistics(len(counters), len(groups))

        if config.test:
            config.sem_test.release()

        log_wrapper.write(f'* Analysis\t\t|\tend with {len(counters)} representative counters')
        