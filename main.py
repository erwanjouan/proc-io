import os, re, time, subprocess
from datetime import datetime
from typing import List
from subprocess import PIPE

sort_by = 'read_bytes'
topn = 10
interval = 10

class IoProcess:
    def __init__(self, pid, datetime, current):
        self.pid = pid
        self.datetime = datetime
        self.current = current
        self.previous = {}
        self.delta = {}

proc_folder = '/proc'
directory = os.fsencode(proc_folder)
io_process_list: List[IoProcess] = []
formatted_line = "{:<8} {:<10} {:<10} {:<10} {:<10} {:<15} {:<15} {:<22} {}\n"

def read_ps():
    ps = {}
    result = subprocess.run(["ps", "-aux"], stdout=PIPE, stderr=PIPE, encoding="utf-8")
    result.check_returncode()
    for line in result.stdout.split('\n'):
        columns = line.split()
        if len(columns) >= 11 and columns[0] != 'USER':
            pid = columns[1]
            ps[pid] = columns
    return ps


def update_io_process_list(pid, datetime, current):
    filtered_list = list(filter(lambda io_process : io_process.pid == pid, io_process_list))
    if(len(filtered_list) == 0):
        io_process = IoProcess(pid, datetime, current)
        io_process_list.append(io_process)
    else:
        io_process = filtered_list[0]
        io_process.previous = io_process.current
        io_process.current = current
        for metric_name in io_process.current.keys():
            io_process.delta[metric_name] = io_process.current[metric_name] - io_process.previous[metric_name] 

def process_file(pid, datetime, path):
    with open(path, 'r') as io_file:
        current = {}
        for line in io_file:
            splitted = line.split()
            metric_name = splitted[0].replace(':','')
            metric_value =  int(splitted[1])
            current[metric_name] = metric_value
        update_io_process_list(pid, datetime, current)

def get_average(metric):
    raw = metric / interval
    if raw > 1000000000:
        return "{:.2f} GB/s".format(raw/1000000000)
    elif raw > 1000000:
        return "{:.2f} MB/s".format(raw/1000000)
    elif raw > 1000:
        return "{:.2f} kB/s".format(raw/1000)
    else :
        return "{:.2f} B/s".format(raw)

def dump_line(io_process, ps, output):
    pid = io_process.pid
    ps_process = ps[pid]
    process_name_args = ' '.join(ps_process[10:len(ps_process)])
    line = formatted_line.format(
        int(pid),
        get_average(io_process.delta["rchar"]),
        get_average(io_process.delta["wchar"]),
        get_average(io_process.delta["syscr"]),
        get_average(io_process.delta["syscw"]),
        get_average(io_process.delta["read_bytes"]),
        get_average(io_process.delta["write_bytes"]),
        get_average(io_process.delta["cancelled_write_bytes"]),
        process_name_args
    )
    output.write(line)


def dump_header(sort_by_metric_name, topn, datetime, output):
    output.write("\n[{}] Top {} process ios ordered by {} desc\n".format(datetime, topn, sort_by_metric_name))
    line = formatted_line.format(
        "PID",
        "rchar",
        "wchar",
        "syscr",
        "syscw",
        "read_bytes",
        "write_bytes",
        "cancelled_write_bytes",
        "process name args"
    )
    output.write(line)


def make_top(sort_by_metric_name, topn, ps, datetime,output):
    io_process_list_with_delta = list(filter(lambda io_process: len(io_process.delta.keys()) > 0, io_process_list))
    if len(io_process_list_with_delta) > 0:
        sorted_io_process_list = sorted(io_process_list_with_delta, key=lambda io_process: int(io_process.delta[sort_by_metric_name]), reverse=True)
        top_io_process = sorted_io_process_list[0:topn]
        dump_header(sort_by_metric_name, topn, datetime,output)
        for io_process in top_io_process:
            dump_line(io_process, ps,output)        

def purge_old_processes(ps):
    global io_process_list
    all_pids = [io_process.pid for io_process in io_process_list]
    for old_pid in all_pids:
        if old_pid not in ps.keys():
            io_process_list = list(filter(lambda io_process : io_process.pid != old_pid, io_process_list))


file_date = datetime.now()
file_name = "/tmp/top-io-{}.txt".format(file_date.strftime("%Y%m%d-%H%M%S"))
with open(file_name, "x") as output:
    while(True):
        datetime = datetime.now()
        ps = read_ps()
        purge_old_processes(ps)
        for pid_str in ps.keys():
            io_file_path = os.path.join(proc_folder, pid_str, 'io')
            if os.path.isfile(io_file_path):
                process_file(pid_str, datetime, io_file_path)
        make_top(sort_by, topn, ps, datetime, output)
        time.sleep(interval)