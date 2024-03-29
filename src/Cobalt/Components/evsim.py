#!/usr/bin/env python

'''Cobalt Event Simulator'''

import ConfigParser
import copy
import logging
import math
import os
import os.path
import random
import signal
import sys
import time
import inspect

from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError
from datetime import datetime
import time

import Cobalt
import Cobalt.Cqparse
import Cobalt.Util
import Cobalt.Components.bgsched

from Cobalt.Components.bgsched import BGSched
from Cobalt.Components.metric_mon import metricmon
from Cobalt.Components.base import Component, exposed, automatic, query, locking
from Cobalt.Components.cqm import QueueDict, Queue
from Cobalt.Components.simulator import Simulator
from Cobalt.Data import Data, DataList
from Cobalt.Exceptions import ComponentLookupError
from Cobalt.Proxy import ComponentProxy, local_components
from Cobalt.Server import XMLRPCServer, find_intended_location

logging.basicConfig()
logger = logging.getLogger('evsim')

no_of_machine = 2
INTREPID = 0
EUREKA = 1
BOTH = 2
MMON = 4
UNHOLD_INTERVAL = 1200
MMON_INTERVAL = 1800

SHOW_SCREEN_LOG = False

CP = ConfigParser.ConfigParser()
CP.read(Cobalt.CONFIG_FILES)
if CP.has_section('evsim') and CP.get("evsim", "no_of_machines"):
    no_of_machine = CP.get("evsim", "no_of_machines")

def sec_to_date(sec, dateformat="%m/%d/%Y %H:%M:%S"):
    tmp = datetime.fromtimestamp(sec)
    fmtdate = tmp.strftime(dateformat)
    return fmtdate

def date_to_sec(fmtdate, dateformat="%m/%d/%Y %H:%M:%S"):
    t_tuple = time.strptime(fmtdate, dateformat)
    sec = time.mktime(t_tuple)
    return sec

class Sim_bg_Sched (BGSched):

    def __init__(self, *args, **kwargs):
        BGSched.__init__(self, *args, **kwargs)

        self.get_current_time = ComponentProxy("event-manager").get_current_time

        predict_scheme = kwargs.get("predict", False)
        if predict_scheme:
            self.running_job_walltime_prediction = bool(int(predict_scheme[2]))
        else:
            self.running_job_walltime_prediction = False

class Sim_Cluster_Sched (BGSched):

    def __init__(self, *args, **kwargs):
        BGSched.__init__(self, *args, **kwargs)
        self.get_current_time = ComponentProxy("event-manager").get_current_time
        self.COMP_QUEUE_MANAGER = "cluster-queue-manager"
        self.COMP_SYSTEM = "cluster-system"
        self.queues = Cobalt.Components.bgsched.QueueDict(self.COMP_QUEUE_MANAGER)
        self.jobs = Cobalt.Components.bgsched.JobDict(self.COMP_QUEUE_MANAGER)
        self.running_job_walltime_prediction = False

class SimEvent (Data):

    """A simulated event

    Attributes:
    machine -- 0, 1, 2 ... represent the system (e.g. Intrepid or Eureka) where the event occurs
    type -- I (init), Q (submit job), S (start job), E (end job),
    datetime -- the date time at which the event occurs
    unixtime -- the unix time form for datetime
    jobid -- the job id associated with the event
    location -- the location where the event occurs, represented by node list or partition list
    """

    fields = Data.fields + [
        "machine", "type", "datetime", "unixtime",
        "jobid", "location",
    ]

    def __init__ (self, spec):
        """Initialize a new partition."""
        Data.__init__(self, spec)
        spec = spec.copy()
        self.machine = spec.get("machine", 0)
        self.type = spec.get("type", "I")
        self.datetime = spec.get("datetime", None)
        self.unixtime = spec.get("unixtime", None)
        self.jobid = spec.get("jobid", 0)
        self.location = spec.get("location", {})

class EventSimulator(Component):
    """Event Simulator. Manages time stamps, events, and the advancing of the clock

    Definition of an event, which is a dictionary of following keys:
        machine -- 0, 1, 2 ... represent the system (e.g. Intrepid or Eureka) where the event occurs
        type -- I (init), Q (submit job), S (start job), E (end job),
        datetime -- the date time at which the event occurs
        unixtime -- the unix time form for datetime
        jobid -- the job id associated with the event
        location -- the location where the event occurs, represented by node list or partition list
    """

    implementation = "evsim"
    name = "event-manager"

    def __init__(self, *args, **kwargs):

        Component.__init__(self, *args, **kwargs)
        self.event_list = [{'unixtime':0}]
        self.time_stamp = 0

        self.finished = False

        ###
        # samnickolay
        self.jobs_queue_time_utilizations = {}
        self.utilization_records = []
        self.next_slowdown_threshold_time_step = None
        self.high_priority_nodes = 0
        self.low_priority_nodes = 0
        # samnickolay
        ###

        # dwang:
        print("[dw_evsim] simu_name: %s. " %kwargs.get("name"))
        print("[dw_evsim] simu_times: %d. " %kwargs.get("times"))
        print("[dw_evsim] checkpoint: %s. " %kwargs.get("checkpoint"))
        #
        print("[dw_evsim] checkp_dsize: %s. " %kwargs.get("checkp_dsize"))
        print("[dw_evsim] checkp_bw_write: %s. " %kwargs.get("checkp_w_bandwidth"))
        print("[dw_evsim] checkp_bw_read: %s. " %kwargs.get("checkp_r_bandwidth"))
        print("[dw_evsim] checkp_interval: %s. " %kwargs.get("checkp_t_internval"))
        # dwang
        self.bgsched = Sim_bg_Sched(**kwargs)
        #self.csched = Sim_Cluster_Sched()

        self.mmon = metricmon()

        self.go_next = True

    def set_go_next(self, bool_value):
        self.go_next = bool_value
    set_go_next = exposed(set_go_next)

    def get_go_next(self,):
        return self.go_next
    get_go_next = exposed(get_go_next)

    def events_length(self):
        return len(self.event_list)

    def add_event(self, ev_spec):
        '''insert time stamps in the same order'''

        time_sec = ev_spec.get('unixtime')
        if time_sec == None:
            # print "insert time stamp error: no unix time provided"
            return -1

        if not ev_spec.has_key('jobid'):
            ev_spec['jobid'] = 0
        if not ev_spec.has_key('location'):
            ev_spec['location'] = []

        pos  = self.events_length()

        while time_sec < self.event_list[pos-1].get('unixtime'):
            pos = pos - 1

        self.event_list.insert(pos, ev_spec)
        #print "insert time stamp ", ev_spec, " at pos ", pos
        return pos
    add_event = exposed(add_event)

    # dwang:
    def del_event(self, jobid_sel):
        for temp_elem in self.event_list:
            if temp_elem.get('jobid') == jobid_sel:
                # print "[DEL_EVENT] temp_elem j_id: ", temp_elem.get('jobid')
                # print "[DEL_EVENT] temp_elem j_loc: ", temp_elem.get('location')
                # print "[DEL_EVENT] temp_elem j_unixt: ", temp_elem.get('unixtime')
                # print "[DEL_EVENT] temp_elem type: ", temp_elem.get('type')
                self.event_list.remove(temp_elem)
    del_event = exposed(del_event)
    # dwang

    def get_time_span(self):
        '''return the whole time span'''
        starttime = self.event_list[1].get('unixtime')
        endtime = self.event_list[-1].get('unixtime')
        timespan = endtime - starttime
        return timespan
    get_time_span = exposed(get_time_span)

    def get_current_time_stamp(self):
        '''return current time stamp'''
        return self.time_stamp

    def get_current_time(self):
        '''return current unix time'''
        return self.event_list[self.time_stamp].get('unixtime')
    get_current_time = exposed(get_current_time)

    def get_current_date_time(self):
        '''return current date time'''
        return self.event_list[self.time_stamp].get('datetime')
    get_current_date_time = exposed(get_current_date_time)

    def get_current_event_type(self):
        '''return current event type'''
        return self.event_list[self.time_stamp].get('type')
    get_current_event_type = exposed(get_current_event_type)

    def get_current_event_job(self):
        '''return current event job'''
        return self.event_list[self.time_stamp].get('jobid')
    get_current_event_job = exposed(get_current_event_job)

    def get_current_event_location(self):
        return self.event_list[self.time_stamp].get('location')
    get_current_event_location = exposed(get_current_event_location)

    def get_current_event_machine(self):
        '''return machine which the current event belongs to'''
        return self.event_list[self.time_stamp].get('machine')

    def get_current_event_all(self):
        '''return current event'''
        return self.event_list[self.time_stamp]

    def get_next_event_time_sec(self):
        '''return the next event time'''
        if self.time_stamp < len(self.event_list) - 1:
            return self.event_list[self.time_stamp + 1].get('unixtime')
        else:
            return -1
    get_next_event_time_sec = exposed(get_next_event_time_sec)


    def is_finished(self):
        return self.finished
    is_finished = exposed(is_finished)

    def clock_increment(self):
        '''the current time stamp increments by 1'''
        if self.time_stamp < len(self.event_list) - 1:
            self.time_stamp += 1
            if SHOW_SCREEN_LOG:
                print str(self.get_current_date_time()) + \
                "[%s]: Time stamp is incremented by 1, current time stamp: %s " % (self.implementation, self.time_stamp)
        else:
            self.finished = True

        return self.time_stamp
    clock_intrement = exposed(clock_increment)

    def add_init_events(self, jobspecs, machine_id):   ###EVSIM change here
        """add initial submission events based on input jobs and machine id"""

        for jobspec in jobspecs:
            evspec = {}
            evspec['machine'] = machine_id
            evspec['type'] = "Q"
            evspec['unixtime'] = float(jobspec.get('submittime'))
            evspec['datetime'] = sec_to_date(float(jobspec.get('submittime')))
            evspec['jobid'] = jobspec.get('jobid')
            evspec['location'] = []
            self.add_event(evspec)
    add_init_events = exposed(add_init_events)


    def init_unhold_events(self, machine_id):
        """add unholding event"""
        if not self.event_list:
            return

        first_time_sec = self.event_list[1]['unixtime']
        last_time_sec = self.event_list[-1]['unixtime']

        unhold_point = first_time_sec + UNHOLD_INTERVAL + machine_id
        while unhold_point < last_time_sec:
            evspec = {}
            evspec['machine'] = machine_id
            evspec['type'] = "C"
            evspec['unixtime'] = unhold_point
            evspec['datetime'] = sec_to_date(unhold_point)
            self.add_event(evspec)

            unhold_point += UNHOLD_INTERVAL + machine_id
    init_unhold_events = exposed(init_unhold_events)

    def init_mmon_events(self):
        """add metrics monitor points into time stamps"""
        if not self.event_list:
            return

        first_time_sec = self.get_first_mmon_point(self.event_list[1]['datetime'])
        last_time_sec = self.event_list[-1]['unixtime']
        machine_id = MMON

        mmon_point = first_time_sec + MMON_INTERVAL
        while mmon_point < last_time_sec:
            evspec = {}
            evspec['machine'] = machine_id
            evspec['unixtime'] = mmon_point
            evspec['datetime'] = sec_to_date(mmon_point)
            self.add_event(evspec)
            mmon_point += MMON_INTERVAL
    init_mmon_events = exposed(init_mmon_events)

    def get_first_mmon_point(self, date_time):
        "based on the input date time (%m/%d/%Y %H:%M:%S), get the next epoch time that is at the beginning of an hour"
        segs = date_time.split()
        hours = segs[1].split(":")
        new_datetime = "%s %s:%s:%s" %  (segs[0], hours[0], '00', '00')
        new_epoch = date_to_sec(new_datetime) + 3600
        return new_epoch

    def print_events(self):
        print "total events:", len(self.event_list)
        i = 0
        for event in self.event_list:
            print event
            i += 1
            if i == 25:
                break

    # dwang:
    #def event_driver(self, preempt):
    #def event_driver(self, preempt,simu_name,simu_tid):
    def event_driver(self, preempt,fp_backf,fp_pre_bj, checkpoint_opt, checkp_dsize, checkp_w_bandwidth, checkp_r_bandwidth,
                     checkp_t_internval, checkp_t_internval_pcent, checkp_heur_opt, job_length_type, checkp_overhead_percent):
    # dwang
        """core part that drives the clock""" 
        # print "[dw_evsim] current t_stamp: ", self.time_stamp
        # print "[dw_evsim] total t_stamp: ", len(self.event_list)
        # print "[dw_evsim] checkpoint_opt: ", checkpoint_opt
        
        if self.go_next:
            ##
            # samnickolay
            # if there is a rt job that will reach the slowdown threshold before the next scheduled time step
            # insert a fake time step and then clear the next_slowdown_threshold_time_step variable
            if self.next_slowdown_threshold_time_step is not None and \
                            self.get_next_event_time_sec() > self.next_slowdown_threshold_time_step and \
                            self.get_current_time() < self.next_slowdown_threshold_time_step:
                evspec = {}
                evspec['unixtime'] = self.next_slowdown_threshold_time_step
                evspec['datetime'] = sec_to_date(self.next_slowdown_threshold_time_step)
                evspec['machine'] = INTREPID

                self.add_event(evspec)
                # global next_slowdown_threshold_time_step
                self.next_slowdown_threshold_time_step = None

            # record the utilization
            current_utilization = ComponentProxy("system").get_utilization_rate(0)
            old_time = self.bgsched.get_current_time()

            #only if the go_next tag is true will the clock be incremented. enable scheduler schedule multiple job at the same time stamp
            self.clock_increment()

            new_time = self.bgsched.get_current_time()
            from bqsim import TOTAL_NODES
            utilization_chunk = (old_time, new_time, current_utilization, self.low_priority_nodes, self.high_priority_nodes)
                                 # float(self.low_priority_nodes)/TOTAL_NODES, float(self.high_priority_nodes)/TOTAL_NODES)

            # from bqsim import utilization_records
            # global utilization_records

            if old_time > 0.0:
                self.utilization_records.append(utilization_chunk)

            if self.time_stamp % 50 == 0:
                print "t=" + str(self.time_stamp) + ' (' + str(len(self.event_list)) + ')'

            # record utilizations for when a job first arrives
            cur_event = self.get_current_event_type()
            if cur_event == "Q":
                cur_event_job = self.get_current_event_job()
                # from bqsim import jobs_queue_time_utilizations
                self.jobs_queue_time_utilizations[int(cur_event_job)] = current_utilization

            # samnickolay
            ###

            # print "[dw_evsim] current t_stamp: ", self.time_stamp
            # print "[dw_evsim] total t_stamp: ", len(self.event_list)


        machine = self.get_current_event_machine()
# 
#         print "event_[%s]: %s, machine=%s, event=%s, job=%s" % (
#                                            self.implementation,
#                                            self.get_current_date_time(),
#                                            self.get_current_event_machine(),
#                                            self.get_current_event_type(),
#                                            self.get_current_event_job(),
#                                            )
#
        if machine == INTREPID:
            # dwang:
            #self.bgsched.schedule_jobs(preempt)
            #self.bgsched.schedule_jobs(preempt,simu_name,simu_tid)

            slowdown_threshold_time_step = None

            # basic_highpQ
            if checkpoint_opt == "highpQ":
                self.bgsched.schedule_jobs(preempt,fp_backf)
            # highpQ_resv 
            elif checkpoint_opt == "highpQ_resv":
                self.bgsched.schedule_jobs_hpQ_resv(preempt,fp_backf)
            # wcheckp
            elif checkpoint_opt == "v0":
                self.bgsched.schedule_jobs_wcheckp_v0(preempt,fp_backf)
            # wcheckp_jrestart
            elif checkpoint_opt == "v1":
                self.bgsched.schedule_jobs_wcheckp_v1(preempt,fp_backf,fp_pre_bj,
                                                        checkp_heur_opt) 
            elif checkpoint_opt == "v1H_wth":
                self.bgsched.schedule_jobs_wcheckp_v1H_wth(preempt,fp_backf,fp_pre_bj,
                                                            checkp_heur_opt) 
            # wcheckp_jresume
            elif checkpoint_opt == "v2":
                self.bgsched.schedule_jobs_wcheckp_v2(preempt,fp_backf,fp_pre_bj,
                                                        checkp_dsize, checkp_w_bandwidth, checkp_r_bandwidth,
                                                        checkp_heur_opt)
            # wcheckp_jresume
            elif checkpoint_opt == "v2p":
                self.bgsched.schedule_jobs_wcheckp_v2p(preempt,fp_backf,fp_pre_bj,
                                                        checkp_dsize, checkp_w_bandwidth, checkp_r_bandwidth, checkp_t_internval,
                                                        checkp_heur_opt)
            # wcheckp_jresume
            elif checkpoint_opt == "v2p_app":
                self.bgsched.schedule_jobs_wcheckp_v2p_app(preempt,fp_backf,fp_pre_bj,
                                                            checkp_dsize, checkp_w_bandwidth, checkp_r_bandwidth, checkp_t_internval_pcent,
                                                            checkp_heur_opt)

            # ...
            # dwang

            ###
            # samnickolay
            elif checkpoint_opt == "baseline":
                self.bgsched.schedule_jobs_baseline(preempt, fp_backf)


            elif checkpoint_opt == "v2_sam_v1":
                results = self.bgsched.schedule_jobs_wcheckp_v2_sam_v1(preempt, fp_backf, fp_pre_bj,
                                                             checkp_dsize, checkp_w_bandwidth, checkp_r_bandwidth,
                                                             checkp_heur_opt, job_length_type)

            elif checkpoint_opt == "v2p_sam_v1":
                results = self.bgsched.schedule_jobs_wcheckp_v2p_sam_v1(preempt,fp_backf,fp_pre_bj, checkp_dsize, checkp_w_bandwidth,
                                                          checkp_r_bandwidth, checkp_t_internval, checkp_heur_opt,
                                                              job_length_type)
            elif checkpoint_opt == "v2p_app_sam_v1":
                results = self.bgsched.schedule_jobs_wcheckp_v2p_app_sam_v1(preempt, fp_backf, fp_pre_bj, checkp_dsize,
                                                                  checkp_w_bandwidth, checkp_r_bandwidth, checkp_heur_opt,
                                                                  job_length_type, checkp_overhead_percent)
            try:
                if results is not None:
                    slowdown_threshold_time_step, low_priority_queue_jobs, high_priority_queue_jobs = results

                if slowdown_threshold_time_step is not None:
                    # global next_slowdown_threshold_time_step
                    self.next_slowdown_threshold_time_step = slowdown_threshold_time_step

                if low_priority_queue_jobs is not None:
                    low_priority_nodes = []
                    for jobid, low_priority_queue_job in low_priority_queue_jobs.iteritems():
                        # low_priority_nodes += int(low_priority_queue_job.nodes)
                        low_priority_nodes.append(low_priority_queue_job.nodes)
                    self.low_priority_nodes = low_priority_nodes

                if high_priority_queue_jobs is not None:
                    high_priority_nodes = []
                    for jobid, high_priority_queue_job in high_priority_queue_jobs.iteritems():
                        # high_priority_nodes += int(high_priority_queue_job.nodes)
                        high_priority_nodes.append(high_priority_queue_job.nodes)
                    self.high_priority_nodes = high_priority_nodes
            except:
                pass

            # samnickolay
            ###


        if machine == EUREKA:
            self.csched.schedule_jobs()
        if machine == MMON:
            self.mmon.metric_monitor()

        if self.go_next:
            ComponentProxy("queue-manager").calc_loss_of_capacity()











