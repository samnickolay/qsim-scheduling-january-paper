# job class
from datetime import datetime, timedelta, time
import csv
import numpy as np

# csv_file_name = 'ANL-ALCF-DJC-MIRA_20170101_20171231.csv'
# csv_file_name = 'ANL-ALCF-DJC-MIRA_20180101_20180731.csv'

file_name = 'mira_dec_wk1.log'

total_nodes = 49152
TOTAL_NODES = 49152 #MIRA
bounded_slowdown_threshold = 10.0 * 60.0  # 10 minutes in seconds


def main():
    jobs = parse_jobs(file_name)

    start_datetime1 = datetime(year=2014, month=12, day=1)
    end_datetime1 = start_datetime1 + timedelta(days=7)
    metrics_start_datetime = datetime(year=2014, month=12, day=2)
    metrics_end_datetime = metrics_start_datetime + timedelta(days=5)
    compute_job_metrics(jobs, start_datetime1, end_datetime1, metrics_start_datetime, metrics_end_datetime, slowdownThreshold=False)

    print('\ndone!')
    # week 2018-01-22 (81%), 2018-03-19 (89%), 2018-06-11 (95%)  


def compute_slowdown(job):
    bounded_run_time = max(10 * 60.0, job.RUNTIME_SECONDS)
    slowdown = ((
                    job.START_TIMESTAMP - job.QUEUED_TIMESTAMP).total_seconds() + bounded_run_time) / bounded_run_time
    return slowdown


def parseline(line):
    '''parse a line in work load file, return a temp
    dictionary with parsed fields in the line'''
    temp = {}
    firstparse = line.split(';')
    temp['EventType'] = firstparse[1]
    # if temp['EventType'] == 'Q':
    #     temp['submittime'] = firstparse[0]
    temp['jobid'] = firstparse[2]
    substr = firstparse.pop()
    if len(substr) > 0:
        secondparse = substr.split(' ')
        for item in secondparse:
            tup = item.partition('=')
            if not temp.has_key(tup[0]):
                temp[tup[0]] = tup[2]
    return temp


def parse_work_load(filename):
    '''parse the whole work load file, return a raw job dictionary'''
    temp = {'jobid':'*', 'submittime':'*', 'queue':'*',
            'Resource_List.walltime':'*','nodes':'*', 'runtime':'*'}
    # raw_job_dict = { '<jobid>':temp, '<jobid2>':temp2, ...}
    raw_job_dict = {}
    wlf = open(filename, 'r')
    for line in wlf:
        line = line.strip('\n')
        line = line.strip('\r')
        if line[0].isdigit():
            temp = parseline(line)
        else:
            # temp = parseline_alt(line)
            print('error')
            exit(-1)
        jobid = temp['jobid']
        #new job id encountered, add a new entry for this job
        if not raw_job_dict.has_key(jobid):
            raw_job_dict[jobid] = temp
        else:  #not a new job id, update the existing entry
            raw_job_dict[jobid].update(temp)

    return raw_job_dict


def compute_job_metrics(jobs, start_datetime, end_datetime, metrics_start_datetime, metrics_end_datetime, slowdownThreshold=False):
    jobs_in_window = []

    # if slowdownThreshold is True:
    #     # compute the slowdown threshold for the jobs in the window
    #     slowdown_values = [compute_slowdown(job) for job in jobs_in_window]
    #     slowdown_threshold = np.percentile(slowdown_values, 95)
    #
    #     # remove any jobs that have slowdowns above the threshold
    #     new_jobs_in_window = []
    #     for job in jobs_in_window:
    #         if compute_slowdown(job) < slowdown_threshold:
    #             new_jobs_in_window.append(job)
    #
    #     old_jobs_in_window_len = len(jobs_in_window)
    #     jobs_in_window = new_jobs_in_window

    # get utilization for all jobs that have low enough slowdown, regardless of window
    utilization_jobs = jobs
    # if slowdownThreshold is True:
    #     for job in jobs:
    #         if compute_slowdown(job) < slowdown_threshold:
    #             utilization_jobs.append(job)
    # else:
    #     utilization_jobs = jobs

    slowdowns = []
    turnaround_times = []
    runtimes = []
    slowdown_category = {'narrow_short': [], 'narrow_long': [], 'wide_short': [], 'wide_long': []}
    turnaround_times_category = {'narrow_short': [], 'narrow_long': [], 'wide_short': [], 'wide_long': []}
    category_count = {'narrow_short': 0, 'narrow_long': 0, 'wide_short': 0, 'wide_long': 0}

    # compute metrics for all low SD jobs in the window
    for job in jobs:
        if job.START_TIMESTAMP <= metrics_start_datetime or job.END_TIMESTAMP >= metrics_end_datetime:
            continue

        slowdown = compute_slowdown(job)
        turnaround_time = (job.END_TIMESTAMP - job.QUEUED_TIMESTAMP).total_seconds()

        slowdowns.append(slowdown)
        turnaround_times.append(turnaround_time)
        runtimes.append(job.RUNTIME_SECONDS)

        if job.NODES_USED <= 4096:  # if job is narrow
            if job.RUNTIME_SECONDS <= 120 * 60.0:  # if job is short
                job_category = 'narrow_short'
            else:  # if job is long
                job_category = 'narrow_long'
        else:  # if job is wide
            if job.RUNTIME_SECONDS <= 120 * 60.0:  # if job is short
                job_category = 'wide_short'
            else:  # if job is long
                job_category = 'wide_long'

        slowdown_category[job_category].append(slowdown)
        turnaround_times_category[job_category].append(turnaround_time)
        category_count[job_category] += 1


    all_slowdown_values = [compute_slowdown(job) for job in jobs_in_window]
    all_slowdown_values.sort()
    t = all_slowdown_values[::-1]

    avg_slowdown = sum(slowdowns) / float(len(slowdowns))
    avg_turnaround_time = sum(turnaround_times) / float(len(turnaround_times)) / 60.0
    avg_runtime = sum(runtimes) / float(len(runtimes)) / 60.0
    print('')
    print('system utilization % ' + str(get_utilization_over_window(utilization_jobs, start_datetime+timedelta(days=1), start_datetime+timedelta(days=6))))
    # print('system utilization % ' + str(get_utilization_over_window(jobs, start_datetime+timedelta(days=1), start_datetime+timedelta(days=6))))
    print('avg bounded slowdown ' + str(avg_slowdown))
    print('avg turnaround_times (min) ' + str(avg_turnaround_time))
    print('avg runtime (min) ' + str(avg_runtime))
    print('max slowdown: ' + str(max(slowdowns)))

    print('')
    for category in ['narrow_short', 'narrow_long', 'wide_short', 'wide_long']:
        print(float(sum(slowdown_category[category])) / len(slowdown_category[category]))
        print(float(sum(turnaround_times_category[category])) / len(turnaround_times_category[category]) / 60.0)
        print(category_count[category])
        print('')

    # if slowdownThreshold is True:
    #     print('slowdown threshold (95%) ' + str(slowdown_threshold))
    #     print('orig jobs len: ' + str(old_jobs_in_window_len))
    #     print('new jobs len: ' + str(len(new_jobs_in_window)))


#
# def make_job_cobalt_log_strings(job):
#     hours, remainder = divmod(job.WALLTIME_SECONDS, 3600)
#     minutes, seconds = divmod(remainder, 60)
#     wall_time_time = str('%02d:%02d:%02d' % (hours, minutes, seconds))
#
#     def make_timestamp(tmp_datetime):
#         epoch = datetime.utcfromtimestamp(0)
#         return (tmp_datetime - epoch).total_seconds()
#
#     output_string1 = job.START_TIMESTAMP.strftime('%m/%d/%Y %H:%M:%S') + ';S;' + str(job.COBALT_JOBID)
#     output_string1 += ';queue=default qtime=' + str(make_timestamp(job.QUEUED_TIMESTAMP))
#     output_string1 += ' Resource_List.nodect=' + str(int(job.NODES_USED))
#     output_string1 += ' Resource_List.walltime=' + wall_time_time
#     output_string1 += ' start=' + str(make_timestamp(job.START_TIMESTAMP)) + ' exec_host=' + job.LOCATION
#     output_string1 += ' utilization_at_queue_time=' + str(job.UTILIZATION_AT_QUEUE_TIME)
#
#     output_string2 = job.END_TIMESTAMP.strftime('%m/%d/%Y %H:%M:%S') + ';E;' + str(job.COBALT_JOBID)
#     output_string2 += ';queue=default qtime=' + str(make_timestamp(job.QUEUED_TIMESTAMP))
#     output_string2 += ' Resource_List.nodect=' + str(int(job.NODES_USED))
#     output_string2 += ' Resource_List.walltime=' + wall_time_time + ' start=' + str(make_timestamp(job.START_TIMESTAMP))
#     output_string2 += ' end=' + str(make_timestamp(job.END_TIMESTAMP)) + ' exec_host=' + job.LOCATION
#     output_string2 += ' runtime=' + str(job.RUNTIME_SECONDS) + ' hold=0 overhead=0'
#     output_string2 += ' utilization_at_queue_time=' + str(job.UTILIZATION_AT_QUEUE_TIME)
#
#
#     # output_string1 = job.START_TIMESTAMP.strftime('%m/%d/%Y %H:%M:%S') + ';S;' + str(job.COBALT_JOBID) + ';queue=default qtime=' + str(job.QUEUED_TIMESTAMP.timestamp())
#     # output_string1 += ' Resource_List.nodect=' + str(int(job.NODES_USED)) + ' Resource_List.walltime=' + wall_time_time + ' start=' + str(job.START_TIMESTAMP.timestamp()) + ' exec_host=' + job.LOCATION
#     # output_string1 += ' utilization_at_queue_time=' + str(job.UTILIZATION_AT_QUEUE_TIME)
#     #
#     # output_string2 = job.END_TIMESTAMP.strftime('%m/%d/%Y %H:%M:%S') + ';E;' + str(job.COBALT_JOBID) + ';queue=default qtime=' + str(job.QUEUED_TIMESTAMP.timestamp())
#     # output_string2 += ' Resource_List.nodect=' + str(int(job.NODES_USED)) + ' Resource_List.walltime=' + wall_time_time + ' start=' + str(job.START_TIMESTAMP.timestamp())
#     # output_string2 += ' end=' + str(job.END_TIMESTAMP.timestamp()) + ' exec_host=' + job.LOCATION + ' runtime=' + str(job.RUNTIME_SECONDS) + ' hold=0 overhead=0'
#     # output_string2 += ' utilization_at_queue_time=' + str(job.UTILIZATION_AT_QUEUE_TIME)
#
#     # print(output_string1)
#     # print(output_string2)
#     return [output_string1, output_string2]
#
# # 11/07/2014 22:58:40;S;359758;queue=default qtime=1415414459.0 Resource_List.nodect=4096 Resource_List.walltime=00:20:00 start=1415422720.9 exec_host=MIR-08800-3BFF1-3-4096
# # 11/07/2014 23:02:10;E;359758;queue=default qtime=1415414459.0 Resource_List.nodect=4096 Resource_List.walltime=00:20:00 start=1415422720.9 end=1415422930.000000 exec_host=MIR-08800-3BFF1-3-4096 runtime=209.1 hold=0 overhead=0
#
#

def list_utilization_by_weeks(jobs, start_datetime, end_datetime, increment):
    # start_datetime = datetime(year=2018, month=1, day=8)
    # end_datetime = datetime(year=2018, month=7, day=31)
    one_week = timedelta(days=7)
    # two_week = timedelta(days=14)
    one_day = timedelta(days=1)

    while start_datetime < end_datetime:
        tmp_utilization_week = get_utilization_over_window(jobs, start_datetime, start_datetime + one_week)
        # tmp_utilization_day = get_utilization_over_window(jobs, start_datetime, start_datetime + one_day)
        tmp_utilization_week = round(tmp_utilization_week, 2)
        # tmp_utilization_day = round(tmp_utilization_day, 2)
        utils_by_day_str = ',  utils by day: '
        for i in range(7):
            tmp = get_utilization_over_window(jobs, start_datetime+timedelta(days=i), start_datetime + timedelta(days=i+1))
            utils_by_day_str += str(round(tmp, 2)) + ', '

        print('week: ' + str(start_datetime.date()) + ' - util = ' + str(tmp_utilization_week) + utils_by_day_str)

        # print('One week ', (str(start_datetime),str(start_datetime + one_week), tmp_utilization))
        # print('just monday', get_utilization_over_window(jobs, start_datetime, start_datetime + one_day))
        # start_datetime += one_week
        start_datetime += increment

        # print()


def get_utilization_over_window(jobs, start_time, end_time):
    total_core_hours = 0.0
    for job in jobs:
        if job.START_TIMESTAMP >= start_time and job.END_TIMESTAMP <= end_time:
            total_core_hours += (job.END_TIMESTAMP-job.START_TIMESTAMP).total_seconds() * job.NODES_USED
        elif job.START_TIMESTAMP < start_time and job.END_TIMESTAMP > start_time and job.END_TIMESTAMP <= end_time:
            total_core_hours += (job.END_TIMESTAMP - start_time).total_seconds() * job.NODES_USED
        elif job.START_TIMESTAMP >= start_time and job.START_TIMESTAMP < end_time and job.END_TIMESTAMP > end_time:
            total_core_hours += (end_time - job.START_TIMESTAMP).total_seconds() * job.NODES_USED
    return total_core_hours / ((end_time - start_time).total_seconds() * total_nodes)


def get_utilization_at_time(jobs, current_time):
    total_nodes_used = 0.0
    for job in jobs:
        if job.START_TIMESTAMP <= current_time and job.END_TIMESTAMP >= current_time:
            total_nodes_used += job.NODES_USED
    return float(total_nodes_used) / total_nodes


def parse_jobs(workload_file):
    raw_jobs = parse_work_load(workload_file)

    specs = []

    jobs_log_values = {}
    jobs_list = []

    tag = 0
    temp_num = 0
    for key in raw_jobs:
        # __0508:
        temp_num = temp_num + 1
        # print "[Init] temp_num: ", temp_num
        # _0508
        spec = {}
        tmp = raw_jobs[key]
        spec['jobid'] = tmp.get('jobid')
        spec['queue'] = tmp.get('queue')
        spec['user'] = tmp.get('user')

        if tmp.get('qtime'):
            qtime = float(tmp.get('qtime'))
        else:
            continue
        # if qtime < self.sim_start or qtime > self.sim_end:
        #     continue
        spec['submittime'] = qtime
        # spec['submittime'] = float(tmp.get('qtime'))
        spec['first_subtime'] = spec['submittime']  # set the first submit time

        # spec['user'] = tmp.get('user')
        spec['project'] = tmp.get('account')

        # convert walltime from 'hh:mm:ss' to float of minutes
        format_walltime = tmp.get('Resource_List.walltime')
        spec['walltime'] = 0
        if format_walltime:
            segs = format_walltime.split(':')
            walltime_minuntes = int(segs[0]) * 60 + int(segs[1])
            spec['walltime'] = str(int(segs[0]) * 60 + int(segs[1]))
        else:  # invalid job entry, discard
            continue

        if tmp.get('runtime'):
            spec['runtime'] = tmp.get('runtime')
        elif tmp.get('start') and tmp.get('end'):
            act_run_time = float(tmp.get('end')) - float(tmp.get('start'))
            if act_run_time <= 0:
                continue
            if act_run_time / (float(spec['walltime']) * 60) > 1.1:
                act_run_time = float(spec['walltime']) * 60
            spec['runtime'] = str(round(act_run_time, 1))
        else:
            continue

        if tmp.get('Resource_List.nodect'):
            spec['nodes'] = tmp.get('Resource_List.nodect')
            if int(spec['nodes']) == TOTAL_NODES:
                continue
        else:  # invalid job entry, discard
            continue

        # if self.walltime_prediction:  # *AdjEst*
        #     if tmp.has_key('walltime_p'):
        #         spec['walltime_p'] = int(
        #             tmp.get('walltime_p')) / 60  # convert from sec (in log) to min, in line with walltime
        #     else:
        #         ap = self.get_walltime_Ap(spec)
        #         spec['walltime_p'] = int(spec['walltime']) * ap
        # else:
        spec['walltime_p'] = int(spec['walltime'])

        spec['state'] = 'invisible'
        spec['start_time'] = '0'
        spec['end_time'] = '0'
        # spec['queue'] = "default"
        spec['has_resources'] = False
        spec['is_runnable'] = False
        spec['location'] = tmp.get('exec_host', '')  # used for reservation jobs only
        spec['start_time'] = tmp.get('start', 0)  # used for reservation jobs only
        # dwang:
        spec['restart_overhead'] = 0.0
        #

        job_values = {}
        job_values['queued_time'] = float(spec['submittime'])
        job_values['log_start_time'] = float(tmp.get('start'))
        job_values['log_end_time'] = float(tmp.get('end'))
        job_values['log_run_time'] = float(spec['runtime']) / 60.0
        temp_bounded_runtime = max(float(spec['runtime']), bounded_slowdown_threshold)
        job_values['log_slowdown'] = (job_values['log_start_time'] - job_values['queued_time'] +
                                      temp_bounded_runtime) / temp_bounded_runtime
        job_values['log_turnaround_time'] = (job_values['log_end_time'] - job_values['queued_time']) / 60.0
        job_values['log_queue_time'] = (job_values['log_start_time'] - job_values['queued_time']) / 60.0
        job_values['nodes'] = int(spec['nodes'])
        if 'utilization_at_queue_time' in tmp:
            job_values['log_utilization_at_queue_time'] = float(tmp.get('utilization_at_queue_time'))
        else:
            job_values['log_utilization_at_queue_time'] = -1.0

        jobs_log_values[int(spec['jobid'])] = job_values

        new_job = job(int(spec['jobid']), float(spec['submittime']), float(tmp.get('start')), float(tmp.get('end')),
                      float(spec['runtime']), int(spec['nodes']) )
        jobs_list.append(new_job)

        # add the job spec to the spec list
        specs.append(spec)
    return jobs_list

class job:
    def __init__(self, COBALT_JOBID, QUEUED_TIMESTAMP, START_TIMESTAMP, END_TIMESTAMP, RUNTIME_SECONDS, NODES_USED):
        # idx = 0
        # self.JOB_NAME = entry[idx]
        # idx += 1
        self.COBALT_JOBID = COBALT_JOBID
        # idx += 1
        # self.MACHINE_NAME = entry[idx]
        # idx += 1
        self.QUEUED_TIMESTAMP =  datetime.utcfromtimestamp(QUEUED_TIMESTAMP)
        self.START_TIMESTAMP = datetime.utcfromtimestamp(START_TIMESTAMP)
        self.END_TIMESTAMP = datetime.utcfromtimestamp(END_TIMESTAMP)
        # self.QUEUED_TIMESTAMP = QUEUED_TIMESTAMP
        # self.START_TIMESTAMP = START_TIMESTAMP
        # self.END_TIMESTAMP = END_TIMESTAMP
        self.RUNTIME_SECONDS = RUNTIME_SECONDS
        self.NODES_USED = NODES_USED
        # try:
        #     self.QUEUED_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        # except:
        #     self.QUEUED_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        # idx += 1
        # self.QUEUED_DATE_ID = int(entry[idx])
        # idx += 1
        # try:
        #     self.START_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        # except:
        #     self.START_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        # idx += 1
        # self.START_DATE_ID = int(entry[idx])
        # idx += 1
        # try:
        #     self.END_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        # except:
        #     self.END_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        # idx += 1
        # self.END_DATE_ID = int(entry[idx])
        # idx += 1
        # self.USERNAME_GENID = entry[idx]
        # idx += 1
        # self.PROJECT_NAME_GENID = entry[idx]
        # idx += 1
        # self.QUEUE_NAME = entry[idx]
        # idx += 1
        # self.WALLTIME_SECONDS = float(entry[idx])
        # idx += 1
        # self.RUNTIME_SECONDS = float(entry[idx])
        # idx += 1
        # self.NODES_USED = float(entry[idx])
        # idx += 1
        # self.NODES_REQUESTED = float(entry[idx])
        # idx += 1
        # self.CORES_USED = float(entry[idx])
        # idx += 1
        # self.CORES_REQUESTED = float(entry[idx])
        # idx += 1
        # self.LOCATION = entry[idx]
        # idx += 1
        # self.EXIT_STATUS = int(entry[idx])
        # idx += 1
        # self.ELIGIBLE_WAIT_SECONDS = int(entry[idx])
        # idx += 1
        # self.ELIGIBLE_WAIT_FACTOR = int(entry[idx])
        # idx += 1
        # self.QUEUED_WAIT_SECONDS = int(entry[idx])
        # idx += 1
        # self.QUEUED_WAIT_FACTOR = int(entry[idx])
        # idx += 1
        # self.REQUESTED_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.USED_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.CAPABILITY_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.NONCAPABILITY_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.BUCKETS3_A_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.BUCKETS3_B_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.BUCKETS3_C_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.MACHINE_PARTITION = entry[idx]
        # idx += 1
        # self.EXIT_CODE = int(entry[idx])
        # idx += 1
        # self.MODE = entry[idx]
        # idx += 1
        # self.RESID = int(entry[idx])
        # idx += 1
        # self.DATA_LOAD_STATUS = entry[idx]
        # idx += 1
        # self.CAPABILITY = entry[idx]
        # idx += 1
        # self.SIZE_BUCKETS3 = entry[idx]
        # idx += 1
        # self.PERCENTILE = entry[idx]
        # idx += 1
        # self.NUM_TASKS_SUBBLOCK = int(entry[idx])
        # idx += 1
        # self.NUM_TASKS_CONSECUTIVE = int(entry[idx])
        # idx += 1
        # self.NUM_TASKS_MULTILOCATION = int(entry[idx])
        # idx += 1
        # self.NUM_TASKS_SINGLE = int(entry[idx])
        # idx += 1
        # self.COBALT_NUM_TASKS = int(entry[idx])
        # idx += 1
        # self.IS_SINGLE = int(entry[idx])
        # idx += 1
        # self.IS_CONSECUTIVE = int(entry[idx])
        # idx += 1
        # self.IS_MULTILOCATION = int(entry[idx])
        # idx += 1
        # self.IS_SUBBLOCK = int(entry[idx])
        # idx += 1
        # self.IS_SUBBLOCK_ONLY = int(entry[idx])
        # idx += 1
        # self.IS_MULTILOCATION_ONLY = int(entry[idx])
        # idx += 1
        # self.IS_MULTILOCATION_SUBBLOCK = int(entry[idx])
        # idx += 1
        # self.IS_CONSECUTIVE_ONLY = int(entry[idx])
        # idx += 1
        # self.IS_SINGLE_ONLY = int(entry[idx])
        # idx += 1
        # self.IS_NO_TASKS = int(entry[idx])
        # idx += 1
        # self.IS_OTHER = int(entry[idx])
        # idx += 1
        # self.OVERBURN_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.IS_OVERBURN = int(entry[idx])
        # idx += 1
        #
        # self.UTILIZATION_AT_QUEUE_TIME = -1.0


if __name__== "__main__":
  main()