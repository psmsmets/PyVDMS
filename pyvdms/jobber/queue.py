

# Mandatory imports
from hashlib import sha256
import os
import json
from warnings import warn


# Relative imports
from ..jobber.job import Job


__all__ = ['Queue']


class Queue(object):
    """
    """
    def __init__(self, json_lock: str = None):
        """
        """
        self._lock = json_lock
        self._cron = None
        self._jobs = []
        if json_lock:
            self.read_lock(json_lock)

    def __str__(self):
        """
        """
        return json.dumps(self.to_dict(), indent=4)

    def __len__(self):
        """
        Return the number of Jobs in the Queue object.
        """
        return len(self._jobs)

    def __iter__(self):
        """
        Return a robust iterator for queue.jobs.
        """
        return iter(self._jobs)

    def items(self, user: str = None, status: str = None):
        """
        """
        return list(filter(
            lambda job: job._has_statuscode(status) and job._has_user(user),
            self._jobs
        ))

    def first(self):
        """
        """
        return self._jobs[0]

    @property
    def crontab(self):
        return self._cron

    def set_crontab(self, crontab: str = None):
        self._cron = crontab

    def add(self, job: Job):
        if isinstance(self.find(job), Job):
            warn(f'Job {job.id} already exists. Skipped.')
            return
        if not job._has_status(['JOB_READY']):
            warn(f'Job {job.id} is not ready. Skipped.')
            return
        if not job._has_status(['JOB_SCHEDULED']):
            job._set_status('JOB_SCHEDULED')
        self._jobs.append(job)
        self.sort()

    def remove(self, job):
        if isinstance(job, Job):
            job = job
        elif isinstance(job, str):
            job = self.find(job)
        else:
            raise TypeError('Illegal input type.')

        if job in self._jobs:
            self._jobs.remove(job)
        else:
            warn('Job not in queue: ignored.')

    def find(self, id: str):
        for job in self._jobs:
            if job.id == id:
                return job
        return None

    def sort(self):
        self._jobs = sorted(
            self._jobs, key=lambda job: int(job.priority), reverse=True
        )

    def from_json(self, json_file: str):
        with open(json_file) as file:
            json_jobs = json.load(file)
            for jj in json_jobs:
                self.add(Job(**jj))

    def _hash(self, jobs: list = None):
        hash_obj = sha256(
            str(
                json.dumps(jobs or self.jobs_to_dict(), indent=4)
            ).encode('ascii')
        )
        return hash_obj.hexdigest()

    def jobs_to_dict(self, user=None, status=None, **kwargs) -> list:
        return [job.to_dict(**kwargs)
                for job in self.items(user=user, status=status)]

    def to_dict(self, **kwargs) -> list:
        return dict(
            _readme=('This file locks the jobs to a known state. '
                     'This file is @generated automatically. '
                     'Do not modify!'),
            crontab=self._cron,
            content_hash=self._hash(),
            jobs=self.jobs_to_dict(**kwargs)
        )

    def write_lock(self, json_file: str = None):
        with open(json_file or self._lock, 'w') as file:
            json.dump(self.to_dict(), file, indent=4)

    def read_lock(self, json_file: str = None):
        self._jobs = []
        self._cron = None
        if not os.path.isfile(json_file or self._lock):
            return
        with open(json_file or self._lock) as file:
            json_jobs = json.load(file)
            if json_jobs['content_hash'] != self._hash(json_jobs['jobs']):
                raise ValueError('Content hash does not comply with the '
                                 'lockfile. Did someone modify the lockfile?')
            if 'crontab' in json_jobs:
                self._cron = json_jobs['crontab']
            for jj in json_jobs['jobs']:
                self.add(Job(**jj))

    def list_job_ids(self) -> list:
        return list(job.id for job in self._jobs)

    def scheduled(self):
        return list(filter(lambda job: job.scheduled, self._jobs))

    def processing(self):
        return list(filter(lambda job: job.processing, self._jobs))
