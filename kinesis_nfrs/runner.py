import base64
import json
import logging as logger
import os
import sys
import time
import uuid
from calendar import timegm
from datetime import datetime, timedelta
from math import ceil
from random import randint, random, choice
from string import digits
from threading import Thread

from google.protobuf.internal.encoder import _VarintBytes

from kinesis_nfrs.opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from kinesis_nfrs.opentelemetry.proto.common.v1.common_pb2 import KeyValue, StringKeyValue
from kinesis_nfrs.opentelemetry.proto.metrics.v1.metrics_pb2 import ResourceMetrics, InstrumentationLibraryMetrics, \
    DoubleSummaryDataPoint, Metric

request_size_mb = int(sys.argv[1])
dpm_count = max(5, int(sys.argv[2]))
run_duration_minutes = int(sys.argv[3])
host_url = "https://stag-endpoint2-events.sumologic.net/receiver/v1/kinesis/metric/ZaVnC4dhaV2qSP6oeQH_RGCyiVZPDdemg8tJ9GSfE343iCBmU-bZC3yiwritFNSj5dksWoQTHpRImnYaYh9IIqcPlrd3ofagC0n-oYS4cpBIGxXxnBDPiQ=="

thread_list = []
thread_count_upper = 4

metric_count = dpm_count / 5
file_timestamp = str(datetime.now()).replace(':', '-').replace(" ", "-")[:-7]
logger.basicConfig(filename=f"{dpm_count}DPM-{run_duration_minutes}Minutes-{file_timestamp}.log",
                   filemode='a',
                   format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                   datefmt='%H:%M:%S',
                   level=logger.INFO)

def get_random_string(length=6):
    return ''.join(choice(digits) for _ in range(length))


def get_unix_nano_time_from_now(time_minute=0):
    return timegm((datetime.utcnow() - timedelta(minutes=-time_minute)).utctimetuple()) * 1000000000


def get_metrics_payload(metric_count=1):
    res_metrics = ResourceMetrics()
    res_metrics.instrumentation_library_metrics.append(get_instrumentation_library_metrics(metric_count))

    resource_keys = {
        "cloud.provider": "aws",
        "cloud.account.id": "628269307657",
        "cloud.region": "us-west-1",
        "aws.exporter.arn": f"arn:aws:cloudwatch:us-west-1:628269307657:metric-stream/MyMetric"
    }

    for key, value in resource_keys.items():
        kv = KeyValue()
        kv.key = key
        kv.value.string_value = value
        res_metrics.resource.attributes.append(kv)

    return res_metrics


def generate_metric_data(metric_index):
    metric = Metric()
    metric.unit = "1"
    metric.name = "someLongMetricName"
    random_string = get_random_string()
    metrics_labels_dict = {
        "Namespace": f"someNamespace_{random_string}",
        "MetricName": f"Metric_Name_{random_string}_{metric_index}"
    }

    for dp_index in range(2):
        dbl_dp = DoubleSummaryDataPoint()
        rand_int = randint(1, 10)
        dbl_dp.start_time_unix_nano = get_unix_nano_time_from_now(-1)
        dbl_dp.time_unix_nano = get_unix_nano_time_from_now(0)
        dbl_dp.count = 1
        dbl_dp.sum = round(random() * rand_int, 2)
        for index in range(2):
            value_at_quantile = dbl_dp.ValueAtQuantile()
            value_at_quantile.quantile = float(index)
            value_at_quantile.value = float(round(random() * rand_int + index, 2))
            dbl_dp.quantile_values.append(value_at_quantile)
        if dp_index == 0:
            for key, value in metrics_labels_dict.items():
                str_key_value = StringKeyValue()
                str_key_value.key = key
                str_key_value.value = value
                dbl_dp.labels.append(str_key_value)
        metric.double_summary.data_points.append(dbl_dp)
    return metric


def serialize_message_and_encode_in_b64(message):
    current_message = message.SerializeToString()
    len_bytes = _VarintBytes(message.ByteSize())
    signed_length = len_bytes + current_message
    base64_bytes = base64.b64encode(signed_length)
    return base64_bytes.decode('ascii')


def get_instrumentation_library_metrics(metric_count):
    ilm = InstrumentationLibraryMetrics()
    for index in range(metric_count):
        ilm.metrics.append(generate_metric_data(index))
    return ilm


def get_number_of_requests(dpm_count, request_size_mb=1):
    static_compressed_size_bytes = 253
    compressed_size_bytes_per_metric = 320
    max_count_metric_per_req = int(
        ((request_size_mb * 1024 * 1024) - static_compressed_size_bytes) / compressed_size_bytes_per_metric)

    count_requests = max(1, int(dpm_count/(5 * max_count_metric_per_req)))
    return count_requests, int(min(metric_count, max_count_metric_per_req))


def generate_requests_metrics(count_requests, metrics_count):
    for _ in range(count_requests):
        ex_req = ExportMetricsServiceRequest()
        ex_req.resource_metrics.append(get_metrics_payload(metrics_count))
        serialized_message = serialize_message_and_encode_in_b64(ex_req)

        uid = str(uuid.uuid1())
        json_file = f"out_{get_random_string()}.json"
        data = {"requestId": uid, "timestamp": int(get_unix_nano_time_from_now(-1) / 1000000),
                "records": [{"data": serialized_message}]}
        fh = open(json_file, "w")
        fh.write(json.dumps(data, indent=2))
        fh.close()

        cmd = f'date; curl -X POST -H "Content-Type: application/json" -H "X-Amz-Firehose-Request-Id: {uid}" -T {json_file} {host_url}'
        os.system(cmd)
        logger.info(datetime.now())
        os.remove(json_file)


def ingest_metrics_into_sumo():
    start_time = datetime.now()
    num_req_per_min, metric_count_per_req = get_number_of_requests(dpm_count, request_size_mb)
    num_req_per_thread = min((num_req_per_min / thread_count_upper), 6)

    thread_count = num_req_per_min if num_req_per_min < thread_count_upper else ceil(num_req_per_min/num_req_per_thread)
    logger.info(f"\nStart Time: {start_time}"
                f"\nRunning test for {run_duration_minutes} Minutes"
                f"\nNumber of Requests per Min: {num_req_per_min}"
                f"\nMetrics Count per request: {metric_count_per_req}"
                f"\nNumber of threads:{thread_count}"
                f"\nNumber of requests per thread: {num_req_per_thread}"
                f"\nIngesting Approx: {num_req_per_thread * thread_count * metric_count_per_req * 5}")

    for cur_min in range(run_duration_minutes):
        for index in range(thread_count):
            thread_list.append(Thread(target=generate_requests_metrics, args=(num_req_per_thread, metric_count_per_req,)))
            try:
                thread_list[len(thread_list) - 1].start()
            except:
                logger.info("Exception in ingesting CW metrics")

        loop_start_min = int(datetime.now().minute)
        for thread in thread_list: thread.join()
        logger.info(f"\nElapsed Time: {cur_min + 1} Mins"
                    f"\nIngested Total: {metric_count_per_req * 5 * num_req_per_thread * thread_count * (cur_min + 1)} DPs")
        if run_duration_minutes - cur_min > 1 and datetime.now().minute - loop_start_min < 1:
            rest_time = 60 - datetime.now().second
            logger.info(f"Sleeping for {rest_time} seconds until next iteration")
            time.sleep(rest_time)

    logger.info(f"\n*****************************\n"
                f"        Run Completed\n"
                f"*****************************\n"
                f"Start Time: {start_time}\n"
                f"End Time: {datetime.now()}\n"
                f"Run run_duration_minutes: {run_duration_minutes}\n"
                f"DPM rate: {dpm_count}\n"
                f"Total Data points Ingested: {metric_count_per_req * 5 * num_req_per_thread * thread_count * run_duration_minutes}")


rest_time = 60 - datetime.now().second
logger.info(f"{datetime.now()} Sleeping for {rest_time} seconds before starting ingestion")
time.sleep(rest_time)
ingest_metrics_into_sumo()
