#!/usr/bin/env python
# coding: utf-8

# Mandatory imports
import json
import os
import sys
import getopt
import re
import platform
from tabulate import tabulate
from crontab import CronTab
from datetime import datetime, timedelta

# Relative imports
from ..version import version as __version__
from ..jobber import Job, Queue


__all__ = []


tabulate_keys = {
    'id': 'id',
    'starttime': 'start',
    'endtime': 'end',
    'time': 'time',
    'station': 'stat',
    'channel': 'chan',
    'priority': '^',
    'status': 'status',
    'max_request_size': 'lim',
    'user': 'user',
}

conf_keys = {
    'starttime': str,
    'endtime': str,
    'station': str,
    'channel': str,
    'sds_root': str,
    'priority': int,
    'max_request_size': str,
    'email': list,
    'client': str,
    'client_kwargs': dict,
}

system = None
home = None
log = None
lock = None
conf = None
queue = None
params = dict(user=None, job=None, status=None, homedir=None,
              cronShell=None, cronEnv=None, args=None)


def usage():
    print("client_scheduler <action> "
          "[-d<dir> -j<job> -s<status> -u<user> -h] [args]")
    print("Actions : {}".format(', '.join(actions.keys())))
    print("Options:")
    print("-d,--dir=<homedir>")
    print("-j,--job=<job>")
    print("-s,--status=<status>")
    print("-u,--user=<user>")
    print("-h,--help")


def version():
    print(__version__)


def parse_conf():
    if not os.path.isfile(conf):
        return dict()
    try:
        with open(conf) as f:
            d = json.load(f)
    except Exception as e:
        print(str(e))
        print(f'Cannot parse json-file "{conf}" with default parameters.')
        return dict()
    for key in list(d):
        if key not in conf_keys:
            print(f'Invalid conf key "{key}". Ignored.')
            del d[key]
        elif d[key] is not None and not isinstance(d[key], conf_keys[key]):
            print('Illegal conf value for key "{}":"{}". Ignored.'
                  .format(key, d[key]))
            del d[key]
    return d


def defaults():
    d = parse_conf()
    print(tabulate(list(zip(d.keys(), d.values()))))


def clean():
    for job in queue.items(status=params['status'] or ['completed'],
                           user=params['user']):
        if job.completed:
            queue.remove(job)
    queue.write_lock()
    index()


def reset():
    for job in queue.items(status=params['status'] or ['error'],
                           user=params['user']):
        if job.error:
            job.schedule()
    queue.write_lock()
    index()


def index():
    table = []
    d = queue.jobs_to_dict(keys=list(tabulate_keys.keys()),
                           status=params['status'], user=params['user'])
    for job in d:
        table.append(list(job[key] for key in tabulate_keys))
    print(
        tabulate(
            table, headers=list(value for key, value in tabulate_keys.items())
        ) or 'No jobs found.'
    )


def add():
    d = parse_conf()
    for arg in params['args']:
        key, value = re.split(r"[:=]", arg.rstrip(','))
        d[key] = value
    try:
        job = Job(**d)
    except Exception as e:
        print(str(e))
        print("Cannot create new job. Did you provide the correct arguments?")
        return
    if job.ready:
        queue.add(job)
        queue.write_lock()
        print_job(job)
        index()


def cancel():
    job = queue.find(params['job'])
    if not job:
        print('You should specify a valid job id.')
        return
    if job.processing:
        print('You cannot remove an active job.')
        return
    queue.remove(job)
    print('Removed job with id={} from queue.'.format(job.id))
    queue.write_lock()
    index()


def cron_start(instant: bool = True):
    if queue.crontab:
        print('Crontab already exists.')
        return
    now = datetime.now()
    cmd = (
        "bash -c 'source $HOME/.bashrc; {cmd} cron:run {opt}' >> {log} 2>&1"
        .format(
            cmd=sys.argv[0],
            log=log,
            opt=('--dir=' + params['homedir']) if params['homedir'] else '',
        )
    )
    crontab = CronTab(user=True)
    cronjob = crontab.new(
        command=cmd,
        comment=('Auto created by client_scheduler on {}'
                 .format(now.isoformat()))
    )
    if system == 'Darwin' or system == 'Linux':
        cronjob.every(1).days()
        now = now - timedelta(0, 60+now.second)  # subtract a minute
        if instant:
            now += timedelta(0, 120)
        cronjob.hour.on(now.hour)
        cronjob.minute.on(now.minute)
    else:
        print('crontab does not work in Windows.')
        return
    cronjob.enable()
    crontab.write()
    queue.set_crontab(str(crontab))
    queue.write_lock()
    print('Crontab added.')
    cron_info()


def cron_stop():
    if queue.crontab:
        crontab = CronTab(user=True)
        for cronjob in crontab.find_command('client_scheduler'):
            cronjob.delete()
        crontab.write()
        queue.set_crontab(None)
        queue.write_lock()
        print('Crontab removed.')
    else:
        print('Crontab does not exist.')


def cron_restart(instant: bool = True):
    if queue.crontab:
        cron_stop()
    cron_start(instant)


def cron_info():
    if queue.crontab:
        print(queue.crontab)
    else:
        print('Crontab does not exist.')


def cron_run():
    print("Client_scheduler run triggered by crontab on",
          datetime.now().isoformat())
    run()
    if system == 'Linux' and queue.crontab:
        cron_restart(instant=False)


def run():
    active = queue.processing()
    if len(active) > 0:
        print('Job {} is already active. Please be patient.'
              .format(active[0].id))
        return
    if params['job']:
        job = queue.find(params['job'])
        if not job:
            print('You should specify a valid job id.')
            return
        run_job(job)
    else:
        for job in queue.scheduled():
            run_next = run_job(job)
            print('run_next ' + ('yes' if run_next else 'no'))
            if not run_next:
                break


def info():
    job = queue.find(params['job'])
    if not job:
        print('You should specify a valid job id.')
        return
    print_job(job)


def update():
    job = queue.find(params['job'])
    if not job:
        print('You should specify a valid job id.')
        return
    d = dict()
    for arg in params['args']:
        key, value = re.split(r"[:=]", arg.rstrip(','))
        d[key] = value
    job.update(**d)
    print_job(job, history=False)
    queue.write_lock()


def logs():
    if os.path.isfile(log):
        with open(log, 'r') as f:
            print(f.read())
    else:
        print('Log file {} not found.'.format(log))


def run_job(job: Job, verbose=True):
    run_next = True
    print("Process job:")
    job._set_status('JOB_PROCESSING')
    queue.write_lock()
    if verbose:
        print_job(job)
    try:
        run_next = job.process(update_status=False)
    except Exception as e:
        job._set_status('JOB_ERROR')
        print(e)
    print(job.status[1]+' @ '+job.status[0])
    queue.write_lock()
    return run_next


def print_job(job: Job, history: bool = True):
    job_info = job.to_dict()
    job_info.pop('status', None)
    for key, value in job_info.items():
        print("{:>25} : {}".format(key, value))
    if history:
        print(tabulate(job.history))


def init():
    global system, home, log, lock, conf, queue
    system = platform.system()
    if params['homedir']:
        home = params['homedir']
    else:
        home = 'CLIENT_SCHEDULER_HOME'
        if not os.environ.get(home):
            print("Cannot find the environment variable \"{}\".".format(home))
            sys.exit(2)
        home = os.environ.get(home)
    if not os.path.isdir(home):
        print("Cannot find the directory \"{}\".".format(home))
        sys.exit(2)
    log = os.path.join(home, "log.txt")
    lock = os.path.join(home, "queue.lock")
    conf = os.path.join(home, "defaults.json")
    queue = Queue(lock)


actions = {
    'list': index,
    'add': add,
    'cancel': cancel,
    'clean': clean,
    'cron:stop': cron_stop,
    'cron:start': cron_start,
    'cron:restart': cron_restart,
    'cron:info': cron_info,
    'cron:run': cron_run,
    'run': run,
    'reset': reset,
    'info': info,
    'update': update,
    'logs': logs,
    'defaults': defaults,
    'version': None,
    'help': None,
}


def main():
    global params
    if len(sys.argv) < 2:
        print('You should provide a valid action.')
        usage()
        sys.exit(2)
    action = (sys.argv[1]).lower()
    if action not in actions:
        print('Illegal action.')
        usage()
        sys.exit(2)
    if action == 'help':
        usage()
        sys.exit()
    elif action == 'version':
        version()
        sys.exit()
    try:
        opts, params['args'] = getopt.getopt(
            sys.argv[2:],
            "hj:s:u:d:",
            ["help", "job=", "status=", "user=", "dir="]
        )
    except getopt.GetoptError as e:
        print(str(e))
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        if opt in ("-v", "--version"):
            version()
            sys.exit()
        elif opt in ("-j", "--job"):
            params['job'] = arg
        elif opt in ("-s", "--status"):
            params['status'] = arg.lower().split(',')
        elif opt in ("-u", "--user"):
            params['user'] = arg.lower().split(',')
        elif opt in ("-d", "--dir"):
            params['homedir'] = arg
        else:
            assert False, "unhandled option"
    init()
    try:
        actions[action]()
    except IOError as e:
        print(str(e))
        print("Is the lock file {0} corrupt?".format(lock))
        sys.exit(2)
    except Exception as e:
        print(str(e))
        print("Unexpected error:", sys.exc_info()[0])
        sys.exit(2)


if __name__ == "__main__":
    main()
