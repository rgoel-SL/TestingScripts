import logging
import os
import shutil
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from math import ceil
from random import choice
from string import ascii_letters, digits
from threading import Thread

''' ####################################################################'''
''' python3 local_file_loggen.py 1024 2 /Users/rgoel/Desktop/Test/abcd/ '''
''' ####################################################################'''
ingestion_rate_per_day_gb = int(sys.argv[1])
run_duration_minutes = int(sys.argv[2])
log_dir = sys.argv[3]
''' ####################################################################'''


def create_loggen_path():
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.makedirs(log_dir)


def get_current_time_string():
    return datetime.now().strftime('%-m/%-d/%Y %-I:%-M:%-S.%f %p')


def get_random_string(length=6):
    return ''.join(choice(ascii_letters + digits) for _ in range(length))


def log_to_file(thread_number):
    script_logger.info(f"Thread Number: {thread_number} Start")
    for index in range(log_ingestion_count_per_thread):
        prefix = f"Thread-{thread_number}-{str(index+1).zfill(max_trailing_zero)}-{ingestion_rate_per_day_gb}GB-{random_file_name}"
        data_gen_logger.info(prefix + random_line[:-(len(prefix) + 26)])
    script_logger.info(f"Thread Number: {thread_number} End")


''' ################## Start of Constants ##################'''
thread_count = 4
thread_list = []
max_trailing_zero = 10
one_gb_in_bytes = 1024 ** 3
log_line_size_bytes = 1024

random_line = get_random_string(log_line_size_bytes)
random_file_name = get_random_string(10)
create_loggen_path()

''' #################### End of Constants ###################'''


def set_logger_and_handler(logger_name, path, backup_count=100, max_bytes=one_gb_in_bytes):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(path, backupCount=backup_count, maxBytes=max_bytes)
    logger.addHandler(handler)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    if logger_name == "log2":
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    handler.setFormatter(formatter)

    return logger, handler


log_ingestion_count_per_day = ingestion_rate_per_day_gb * one_gb_in_bytes / log_line_size_bytes
log_ingestion_count_per_min = int(log_ingestion_count_per_day // 1440)
log_ingestion_count_per_thread = ceil(log_ingestion_count_per_min / thread_count)

file_timestamp = str(datetime.now()).replace(':', '-').replace(" ", "-")[:-7]
data_gen_logger, data_gen_handler = set_logger_and_handler("log1", path=os.path.join(log_dir, f"{random_file_name}.log"))
script_logger, _ = set_logger_and_handler("log2", path=f"{ingestion_rate_per_day_gb}GB-"
                                                       f"{run_duration_minutes}Mins-"
                                                       f"{random_file_name}-"
                                                       f"{file_timestamp}.log", backup_count=1)


def generate_logs():
    script_logger.info(f"Starting to Ingest Total "
                       f"{log_ingestion_count_per_thread * thread_count * run_duration_minutes} log lines")
    file_size_mb = log_ingestion_count_per_thread * thread_count * log_line_size_bytes / (1024 ** 2)
    script_logger.info(f"Size of each File: {file_size_mb} MB")
    script_logger.info(f"Total size of files: {file_size_mb * run_duration_minutes} MB")

    for cur_min in range(run_duration_minutes):
        if cur_min > 0:
            data_gen_handler.doRollover()
        for thread_num in range(1, thread_count+1):
            thread_list.append(Thread(target=log_to_file, args=(thread_num,)))
            thread_list[len(thread_list) - 1].start()

        loop_start_min = int(datetime.now().minute)
        for thread in thread_list: thread.join()
        script_logger.info(f"\nElapsed Time: {cur_min + 1} Mins"
              f"\nGenerated Total: {log_ingestion_count_per_thread * thread_count * (cur_min + 1)} Log Lines")

        if run_duration_minutes - cur_min > 1 and datetime.now().minute - loop_start_min < 1:
            rest_time = 60 - datetime.now().second
            script_logger.info(f"Sleeping for {rest_time} seconds until next iteration")
            time.sleep(rest_time)

    script_logger.info("Ingestion Complete")
    script_logger.info(f"Start Time: {start_time}")
    script_logger.info(f"End Time: {datetime.now()}")
    script_logger.info(f"Ingested total lines {log_ingestion_count_per_thread * thread_count * run_duration_minutes}")
    script_logger.info(f"Prefix used: {random_file_name}")
    script_logger.info(f"No of files: {run_duration_minutes}")
    script_logger.info(f"Size of each File: {file_size_mb} MB")
    script_logger.info(f"Total size of generated files: {file_size_mb * run_duration_minutes} MB")

    
rest_time = 60 - datetime.now().second
start_time = datetime.now()
script_logger.info(f"Starting Script")
script_logger.info(f"Sleeping for {rest_time} seconds before starting ingestion")
time.sleep(rest_time)
generate_logs()
