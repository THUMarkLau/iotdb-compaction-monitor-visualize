import configparser
import logging
import time
from datetime import datetime

import matplotlib.pyplot as plt
from iotdb.Session import Session

CONFIG_PATH = "config.ini"
config_parser = configparser.ConfigParser()
config_parser.read(CONFIG_PATH)
session = None
logger = logging.getLogger("Compaction-Monitor-Visualization")
start_time = int(
    time.mktime(time.strptime(config_parser.get("Visualize", "StartDate"), '%Y-%m-%d')) * 1000)
end_time = int(
    time.mktime(time.strptime(config_parser.get("Visualize", "EndDate"), '%Y-%m-%d')) * 1000)


def init_session():
    global session
    session = Session(config_parser.get("IoTDB", "host"), config_parser.get("IoTDB", "port"),
                      config_parser.get("IoTDB", "user"), config_parser.get("IoTDB", "password"))
    session.open(False)


def main():
    init_session()
    try:
        check_monitor_sg()
        visualize_cpu()
    finally:
        session.close()


def check_monitor_sg():
    dataset = session.execute_query_statement("show storage group")
    monitor_sg_found = False
    while dataset.has_next():
        row = dataset.next()
        if row.get_fields()[0].get_string_value() == "root.compaction_monitor":
            monitor_sg_found = True
            break
    dataset.close_operation_handle()
    if not monitor_sg_found:
        logger.error("Cannot find the compaction monitor sg")
        exit(-1)
    else:
        logger.info("Compaction monitor sg found")


def collect_cpu_timeseries_name():
    dataset = session.execute_query_statement("show timeseries root.compaction_monitor")
    compaction_cpu_threads_name = []
    merge_cpu_threads_name = []
    while dataset.has_next():
        row = dataset.next()
        timeseries_name = row.get_fields()[0].get_string_value()
        if timeseries_name.startswith("root.compaction_monitor.compaction.cpu"):
            compaction_cpu_threads_name.append(timeseries_name)
        elif timeseries_name.startswith("root.compaction_monitor.merge.cpu"):
            merge_cpu_threads_name.append(timeseries_name)

    dataset.close_operation_handle()
    return compaction_cpu_threads_name, merge_cpu_threads_name


def collect_cpu_cost(compaction_timeseries_name):
    compaction_cpu_sql = "select * from root.compaction_monitor.compaction.cpu"
    dataset = session.execute_query_statement(compaction_cpu_sql)
    compaction_cpu_consumption = {}
    timestamps = []
    compaction_cpu_index_map = {}
    column_names = dataset.get_column_names()
    compaction_cpu_consumption["Compaction-Total"] = []
    for i in range(1, len(column_names)):
        compaction_cpu_consumption[column_names[i]] = []
        compaction_cpu_index_map[column_names[i]] = i - 1

    try:
        while dataset.has_next():
            row = dataset.next()
            s = 0
            cur_time = row.get_timestamp()
            if row.get_timestamp() < start_time:
                continue
            elif row.get_timestamp() > end_time:
                return timestamps, compaction_cpu_consumption
            for ts_name in compaction_timeseries_name:
                compaction_cpu_consumption[ts_name].append(
                    row.get_fields()[compaction_cpu_index_map[ts_name]].get_float_value() * 100)
                s += row.get_fields()[compaction_cpu_index_map[ts_name]].get_float_value() * 100
            compaction_cpu_consumption["Compaction-Total"].append(s)
            timestamps.append(row.get_timestamp())

        return timestamps, compaction_cpu_consumption
    finally:
        dataset.close_operation_handle()


def process_timestamp(timestamps):
    return list(map(lambda ts: datetime.fromtimestamp(ts / 1000), timestamps))


def visualize_cpu():
    compaction_timeseries_name, merge_timeseries_name = collect_cpu_timeseries_name()
    timestamps, compaction_cpu_consumption = collect_cpu_cost(compaction_timeseries_name)
    timestamps = process_timestamp(timestamps)
    for ts_name in compaction_timeseries_name:
        consumption = compaction_cpu_consumption[ts_name]
        plt.plot(timestamps, consumption, label=ts_name.split(".")[-1])
    consumption = compaction_cpu_consumption["Compaction-Total"]
    plt.plot(timestamps, consumption, label="Compaction-Total", marker="+")
    plt.xlabel("time")
    plt.ylabel("CPU consumption in percentage")
    plt.title("Compaction CPU Consumption")
    plt.legend()
    plt.grid()
    plt.show()


if __name__ == '__main__':
    main()
