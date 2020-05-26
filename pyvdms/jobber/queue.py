

# Mandatory imports
from hashlib import sha256
import os
import json
from warnings import warn
from tabulate import tabulate


# Relative imports
from ..jobber.job import Job


__all__ = ['Queue']

_tabulate_keys = {
    'id': 'id',
    'starttime': 'start',
    'endtime': 'end',
    'time': 'time',
    'station': 'stat',
    'channel': 'chan',
    'priority': '^',
    'status': 'status',
    'request_limit': 'lim',
    'user': 'user',
}


class Queue(object):
    """
    """
    def __init__(self, json_lock: str = None):
        """
        """
        self._lock = json_lock
        self._cron = None
        self.jobs = []
        if json_lock:
            self.read_lock(json_lock)

    def __str__(self):
        """Print the queue overview.
        """
        return self.index()

    def _repr_pretty_(self, p, cycle):
        p.text(self.__str__())

    def __len__(self):
        """
        Return the number of jobs in the queue.
        """
        return len(self.jobs)

    def __iter__(self):
        """
        Return a robust iterator for jobs in the queue.
        """
        return iter(self.jobs)

    def items(self, user: str = None, status: str = None):
        """Return the jobs in the queue, optionally filtered for
        a user and/or status.
        """
        return list(filter(
            lambda job: job._has_statuscode(status) and job._has_user(user),
            self.jobs
        ))

    def index(self, status: str = None, user: str = None):
        """Print the queue overview, optionally filter for a user or status.
        """
        table = []
        jobs = self.jobs_to_dict(keys=list(_tabulate_keys.keys()),
                                 status=status, user=user)

        if not jobs:
            return 'No jobs found.'

        for job in jobs:
            table.append(list(job[key] for key in _tabulate_keys))

        return tabulate(
            table, headers=list(val for key, val in _tabulate_keys.items())
        )

    def next(self):
        """Return the next job to process in the queue.
        """
        return self.scheduled()[0] if self.scheduled() else None

    @property
    def crontab(self):
        """Get or set the job crontab.
        """
        return self._cron

    @crontab.setter
    def crontab(self, crontab: str = None):
        """
        """
        if not (isinstance(crontab, str) or crontab is None):
            raise TypeError('crontab should be of type `str`.')
        self._cron = crontab

    def add(self, job: Job):
        """Add a job to the queue.
        """
        if not isinstance(job, Job):
            raise TypeError('job should be a Job object.')

        if isinstance(self.find(job), Job):
            warn(f'Job {job.id} already exists. Skipped.')
            return

        if not job._has_status(['JOB_READY']):
            warn(f'Job {job.id} is not ready. Skipped.')
            return

        if not job._has_status(['JOB_SCHEDULED']):
            job._set_status('JOB_SCHEDULED')

        self.jobs.append(job)
        self.sort()

    def remove(self, job):
        """Remove a job from the queue by its id.
        """
        if isinstance(job, Job):
            job = job
        elif isinstance(job, str):
            job = self.find(job)
        else:
            raise TypeError('job should be of type `str` or a Job object.')

        if job in self.jobs:
            self.jobs.remove(job)
        else:
            warn('Job not in queue: ignored.')

    def find(self, id: str):
        """Find a job in the queue by its id.
        """
        for job in self.jobs:
            if job.id == id:
                return job
        return None

    def sort(self):
        """Sort the queue on the job priority (descending).
        """
        self.jobs = sorted(
            self.jobs, key=lambda job: int(job.priority), reverse=True
        )

    def process(self, **kwargs):
        """Process the queue until completed or paused.
        """
        while True:

            job = self.next()

            if not job:
                break

            job.process(**kwargs)

            if job.paused:
                break

    def process_next(self, **kwargs):
        """Process the next job in the queue.
        """
        job = self.next()
        if job:
            job.process(**kwargs)

    def from_json(self, json_file: str):
        """Load a queue of jobs from a JSON file.
        """
        with open(json_file) as file:
            jsonjobs = json.load(file)
            for jj in jsonjobs:
                self.add(Job(**jj))

    def _hash(self, jobs: list = None):
        """Hash the queue.
        """
        hash_obj = sha256(
            str(
                json.dumps(jobs or self.jobs_to_dict(), indent=4)
            ).encode('ascii')
        )
        return hash_obj.hexdigest()

    def jobs_to_dict(self, user=None, status=None, **kwargs) -> list:
        """Convert the queued jobs to a list of dictionaries.
        """
        return [job.to_dict(**kwargs)
                for job in self.items(user=user, status=status)]

    def to_dict(self, **kwargs) -> list:
        """Convert the queued jobs and queue parameters to a list of
        dictionaries.
        """
        return dict(
            _readme=('This file locks the jobs to a known state. '
                     'This file is @generated automatically. '
                     'Do not modify!'),
            crontab=self._cron,
            content_hash=self._hash(),
            jobs=self.jobs_to_dict(**kwargs)
        )

    def write_lock(self, json_file: str = None):
        """Write the locked queue to a JSON file.
        """
        with open(json_file or self._lock, 'w') as file:
            json.dump(self.to_dict(), file, indent=4)

    def read_lock(self, json_file: str = None):
        """Read the locked queue to a JSON file.
        """
        self.jobs = []
        self._cron = None
        if not os.path.isfile(json_file or self._lock):
            return
        with open(json_file or self._lock) as file:
            jsonjobs = json.load(file)
            if jsonjobs['content_hash'] != self._hash(jsonjobs['jobs']):
                raise ValueError('Content hash does not comply with the '
                                 'lockfile. Did someone modify the lockfile?')
            if 'crontab' in jsonjobs:
                self._cron = jsonjobs['crontab']
            for jj in jsonjobs['jobs']:
                self.add(Job(**jj))

    def ids(self) -> list:
        """List the job ids in the queue.
        """
        return list(job.id for job in self.jobs)

    def scheduled(self) -> list:
        """List the scheduled jobs in the queue.
        """
        return list(filter(lambda job: job.scheduled, self.jobs))

    def processing(self) -> list:
        """List the processing jobs in the queue.
        """
        return list(filter(lambda job: job.processing, self.jobs))
