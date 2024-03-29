import subprocess
import sys

import argparse

parser = argparse.ArgumentParser(description='Automate running Cobalt simulations')

# input_rt_percent 
# input_rt_job_categories
# input_checkpoint_heuristics
# input_times

# sample script examples to test that it works correctly
# python cobalt-automating-script.py --rt_percents 5 --trials 1 --test
# python cobalt-automating-script.py --rt_percents 5 10 --trials 1 --test
# python cobalt-automating-script.py --rt_percents 5 --trials 1 --rt_job_categories all --test
# python cobalt-automating-script.py --rt_percents 5 --trials 1 --rt_job_categories all short --test
# python cobalt-automating-script.py --rt_percents 5 --trials 1 --checkpoint_heuristics v2p --test
# python cobalt-automating-script.py --rt_percents 5 --trials 1 --checkpoint_heuristics v2p walltime --test
# python cobalt-automating-script.py --rt_percents 5 --trials 1 --rt_job_categories short --checkpoint_heuristics actual --test
# python cobalt-automating-script.py --rt_percents 5 20 --trials 1 --rt_job_categories short short-or-narrow --checkpoint_heuristics walltime actual --test
# python cobalt-automating-script.py --rt_percents 5 --trials 5 --test
# python cobalt-automating-script.py --rt_percents 5 20 --trials 5 --rt_job_categories short short-or-narrow --checkpoint_heuristics walltime actual --test
'''
export PYTHONPATH=/home/samnickolay/cobalt-master/src
export HOST=ubuntu
export PYTHONUNBUFFERED=1
export COBALT_CONFIG_FILES=/home/samnickolay/cobalt-master/src/components/conf/cobalt.conf
cd /home/samnickolay/cobalt-master/src/components

python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script.py --rt_percents 10 --trials 1 --rt_job_categories all --checkpoint_heuristics walltime --test

python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script.py --rt_percents 10 20 --trials 5 --rt_job_categories all short-and-narrow short-or-narrow --checkpoint_heuristics highpQ v2p walltime --test


/usr/bin/python2.7 qsim.py --batch --name 5-all-v2p_sam_v1-walltime --job mira_dec_4.log --backfill ff --times 1 --output 5-all-v2p_sam_v1-walltime.log --partition mira.xml -Y 5 --checkpoint v2p_sam_v1 --checkp_dsize 4096 --checkp_w_bandwidth 216 --checkp_r_bandwidth 216 --checkp_t_internval 900 --intv_pcent 0.01 --utility_function custom_v1 --job_length_type walltime --rt_job_categories all

'''

# python /home/samnickolay/cobalt-automating-script.py --rt_percents 10 --trials 5 --rt_job_categories all --checkpoint_heuristics baseline v2p_app v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 10 --test


# python /home/samnickolay/cobalt-automating-script.py --rt_percents 5 10 --trials 10 --rt_job_categories all --checkpoint_heuristics baseline v2_sam_v1 v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name mira_oct_wk1.log --test

# python /home/samnickolay/cobalt-automating-script.py --rt_percents 10 --trials 1 --rt_job_categories all --checkpoint_heuristics baseline v2_sam_v1 v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name mira_oct_wk1.log --test



# heuristic_to_test = 'v2'
# heuristic_to_test = 'v2'
# checkpoint_heuristics = ['baseline'] + [heuristic_to_test] + ['walltime', 'actual', 'predicted']
# checkpoint_heuristics = ['baseline', 'v2p', 'walltime', 'actual', 'predicted']

# small_simulation_test = False

small_simulation_test_log_files = ['0.log', '1.log', '2.log', '3.log', '4.log', '5.log', '6.log', '7.log', '8.log', '9.log', '10.log', 
                                   '11.log', '12.log', '13.log', '14.log', '15.log', '16.log', '17.log', '18.log', '19.log']
small_simulation_test_log_directory = '/home/samnickolay/create-test-logs/simulation-logs/'

small_simulation_test_log_files = [small_simulation_test_log_directory + log_file for log_file in small_simulation_test_log_files]

small_partition_file_name = 'mira-8k.xml'

import datetime

start_time = datetime.datetime.now()
print('start time: ' + str(start_time))


rt_percents = ['5', '10', '15', '20']
rt_job_categories = ['all', 'short', 'narrow', 'short-and-narrow', 'short-or-narrow', 'corehours']
checkpoint_heuristics = ['baseline', 'highpQ', 'v1', 'v2', 'v2_sam_v1', 'v2p', 'v2p_sam_v1', 'v2p_app', 'v2p_app_sam_v1']
runtime_estimators = ['walltime', 'actual', 'predicted']
checkp_overhead_percents = ['5', '10', '20']

checkp_dsize = '16384'
checkp_t_internval = '3600'
application_checkpointing_percent = '50.0'

# log_file_name = 'mira_dec_4.log'
##### make sure to change this back!
# log_file_name = 'mira_dec_wk1.log'


partition_file_name = 'mira.xml'

# add the arguments to the parser
parser.add_argument('--rt_percents', '-P', dest='input_rt_percents', nargs='+', choices=rt_percents,
                    help='the rt percents to simulate', required=True)

parser.add_argument('--trials', '-T', dest='input_times', type=int,
                    help='the number of trials to simulate for each configuration', required=True)

parser.add_argument('--log_file_name', '-l', dest='log_file_name', type=str,
                    help='the name of the log file to use for the simulations', required=True)

parser.add_argument('--rt_job_categories', '-C', dest='input_rt_job_categories', nargs='+', default=[], choices=rt_job_categories,
                    help="the rt job categories to simulate (default: test all categories " + str(rt_job_categories) + ")")

parser.add_argument('--checkpoint_heuristics', '-H', dest='input_checkpoint_heuristics', nargs='+', default=[], choices=checkpoint_heuristics,
                    help="the checkpoint heuristics to simulate (default: test all heuristics " + str(checkpoint_heuristics) + ")")

parser.add_argument('--runtime_estimators', '-E', dest='input_runtime_estimators', nargs='+', default=[], choices=runtime_estimators,
                    help="the checkpoint heuristics to simulate (default: test all estimators " + str(runtime_estimators) + ")")

parser.add_argument('--checkp_overhead_percents', '-O', dest='input_checkp_overhead_percents', nargs='+', default=[], choices=checkp_overhead_percents,
                    help="the checkpoint overhead % to use with application checkpointing heuristic (default: test all % " + str(checkp_overhead_percents) + ")")

parser.add_argument('--test', '-t', default=False, action='store_true',
                    help="if test flag is passed then the script will not actually run the cobalt simulations")


parser.add_argument('--small_simulation_test', '-s', default=False, action='store_true',
                    help="if small_simulation_test flag is passed then the script will run small simulation traces using small partition file")


parser.add_argument('--results_window_length', dest='results_window_length', type=int, required=True,
                    help="Specifies the length for the results window (number of days) (the time window to compute trimmed metrics)")

parser.add_argument('--results_window_start', dest='results_window_start', type=str, required=True,
                    help="Specifies the start_date for the results window (YYYY-MM-DD) (the time window to compute trimmed metrics)")

# parse the arguments
args = parser.parse_args()

input_rt_percents = args.input_rt_percents
times = str(args.input_times)
input_rt_job_categories = args.input_rt_job_categories
input_checkpoint_heuristics = args.input_checkpoint_heuristics
input_runtime_estimators = args.input_runtime_estimators
input_checkp_overhead_percents = args.input_checkp_overhead_percents
small_simulation_test = args.small_simulation_test
log_file_name = args.log_file_name

results_window_start = args.results_window_start
results_window_length = args.results_window_length

# if no values are passed then default to testing all values
if input_rt_job_categories == []:
    input_rt_job_categories = rt_job_categories

if input_checkpoint_heuristics == []:
    input_checkpoint_heuristics = checkpoint_heuristics

if input_runtime_estimators == []:
    input_runtime_estimators = runtime_estimators

if input_checkp_overhead_percents == []:
    input_checkp_overhead_percents = checkp_overhead_percents

# output the inputted
print("")
print("Simulation Configurations:")
print("trials: " + times)
print("rt_percents: " + str(input_rt_percents))
print("rt_job_categories: " + str(input_rt_job_categories))
print("checkpoint_heuristics: " + str(input_checkpoint_heuristics))
print('runtime_estimators: ' + str(input_runtime_estimators))
print('checkp_overhead_percents: ' + str(checkp_overhead_percents))
print("")
if args.test is True:
    print("!!!!!! This is a test - No cobalt simulations will be run !!!!!!")
print("")

if small_simulation_test:
        print("****** Running small simulation test with " + str(len(small_simulation_test_log_files)) + 
            " small traces using " + small_partition_file_name + " partition file ******")
print("")



def run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function, checkp_overhead_percent='-1'):

    if small_simulation_test:
        current_log_file_names = small_simulation_test_log_files
        current_partition_file_name = small_partition_file_name

    else:
        current_log_file_names = [log_file_name]
        current_partition_file_name = partition_file_name


    for current_log_file_name in current_log_file_names:

        trimmed_current_log_file_name = current_log_file_name.split('/')[-1]
        sim_name = rt_percent + '%-' + rt_job_category + '-' + checkpoint_heuristic

        if 'sam_v1' in checkpoint_heuristic:
            sim_name += '-' + job_length_type
        if checkp_overhead_percent != '-1':
            sim_name += '-' + str(checkp_overhead_percent) + '%_overhead'

        sim_name += '-' + current_partition_file_name + '-' + trimmed_current_log_file_name



        print("--- " + sim_name + " ---")


        command_str = "/usr/bin/python2.7 qsim.py --batch --name " + sim_name + " --job " + current_log_file_name + " --backfill ff"\
        " --times " + times + " --output " + sim_name + ".log --partition " + current_partition_file_name + " -Y " + rt_percent +\
        " --checkpoint " + checkpoint_heuristic + " --checkp_dsize " + checkp_dsize + " --checkp_w_bandwidth 216"\
        " --checkp_r_bandwidth 216 --checkp_t_internval " + checkp_t_internval + " --intv_pcent " + application_checkpointing_percent + " --utility_function " + utility_function +\
        " --job_length_type " + job_length_type + " --rt_job_categories " + rt_job_category + " --overhead_checkpoint_percent " + checkp_overhead_percent

        command_str += " --results_window_length " + str(results_window_length)
        command_str += " --results_window_start " + str(results_window_start)

        print(command_str)
        print('') 

        if args.test is False:
            import os
            os.chdir('/home/samnickolay/cobalt-master/src/components')
            command_list = command_str.split(' ')
            # FNULL = open('/home/samnickolay/output-logs/' + sim_name + '-output-log.txt', 'w')
            # FNULL = open(os.devnull, 'w')
            # retcode = subprocess.call(command_list, stdout=FNULL, stderr=subprocess.STDOUT)

            if not os.path.exists('simulation_output/'):
                os.makedirs('simulation_output/')

            stdout_file = open('simulation_output/' + sim_name + '.output', 'w')
            retcode = subprocess.call(command_list, stdout=stdout_file, stderr=subprocess.STDOUT)
            stdout_file.close()
            print(retcode)


for rt_percent in input_rt_percents:
    print('----------------------------------------------------------------------')
    print("Testing rt percent: " + rt_percent)

    for rt_job_category in input_rt_job_categories:
        print("***********************************")
        print("Testing rt job category: " + rt_job_category  + "\n")

        for checkpoint_heuristic in input_checkpoint_heuristics:

            if checkpoint_heuristic == "baseline":
                job_length_type = "none"
                utility_function = "wfp2"

                run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function)

            elif 'sam_v1' not in checkpoint_heuristic: # ['v2', 'v2p', 'v2p_app']
                job_length_type = "none"
                utility_function = "default"

                run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function)

            elif 'sam_v1' in checkpoint_heuristic: # ['v2_sam_v1', 'v2p_sam_v1', 'v2p_app_sam_v1']
                for runtime_estimator in input_runtime_estimators:
                    job_length_type = runtime_estimator
                    # utility_function = "custom_v1"
                    utility_function = "wfp2"


                    if 'app' in checkpoint_heuristic:
                        for checkp_overhead_percent in input_checkp_overhead_percents:
                            run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function, checkp_overhead_percent)
                    else:

                        run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function)


            # elif checkpoint_heuristic == heuristic_to_test:
            # # elif checkpoint_heuristic == "v2p":
            #   job_length_type = "none"
            #   utility_function = "default"

            # elif checkpoint_heuristic == "walltime":
            #   job_length_type = "walltime"
            #   checkpoint_heuristic = heuristic_to_test + "_sam_v1"
            #   # checkpoint_heuristic = "v2p_sam_v1"
            #   utility_function = "custom_v1"

            # elif checkpoint_heuristic == "actual":
            #   job_length_type = "actual"
            #   checkpoint_heuristic = heuristic_to_test + "_sam_v1"
            #   # checkpoint_heuristic = "v2p_sam_v1"
            #   utility_function = "custom_v1"

            # elif checkpoint_heuristic == "predicted":
            #   job_length_type = "predicted"
            #   checkpoint_heuristic = heuristic_to_test + "_sam_v1"
            #   # checkpoint_heuristic = "v2p_sam_v1"
            #   utility_function = "custom_v1"

            else:
                print('error on checkpoint_heuristic: ', checkpoint_heuristic)
                exit(-1)

            # sim_name = rt_percent + '-' + rt_job_category + '-' + checkpoint_heuristic

            # if 'sam_v1' in checkpoint_heuristic:
            #   sim_name += '-' + job_length_type


            # print("--- " + sim_name + " ---")

            # command_str = "/usr/bin/python2.7 qsim.py --batch --name " + sim_name + " --job mira_dec_4.log --backfill ff"\
            # " --times " + times + " --output " + sim_name + ".log --partition mira.xml -Y " + rt_percent +\
            # " --checkpoint " + checkpoint_heuristic + " --checkp_dsize " + checkp_dsize + " --checkp_w_bandwidth 216"\
            # " --checkp_r_bandwidth 216 --checkp_t_internval " + checkp_t_internval + " --intv_pcent 0.01 --utility_function " + utility_function +\
            # " --job_length_type " + job_length_type + " --rt_job_categories " + rt_job_category

            # print(command_str)
            # print('') 

            # if args.test is False:
               #  import os
               #  os.chdir('/home/samnickolay/cobalt-master/src/components')
               #  command_list = command_str.split(' ')
               #  # FNULL = open('/home/samnickolay/output-logs/' + sim_name + '-output-log.txt', 'w')
               #  FNULL = open(os.devnull, 'w')
               #  retcode = subprocess.call(command_list, stdout=FNULL, stderr=subprocess.STDOUT)
               #  print(retcode)


print('done!')


end_time = datetime.datetime.now()
print('end time: ' + str(end_time))
print('run time: ' + str(end_time-start_time))