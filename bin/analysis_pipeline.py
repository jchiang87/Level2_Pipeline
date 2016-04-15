#!/usr/bin/env python
"""
This needs to be run in a bash environment where
$ setup obs_lsstSim
$ setup -m none -r and_files astrometry_net_data
have been run and which contains an images/ and and_files/
subdirectories.
"""
import os
import sys
import logging
from collections import OrderedDict
import subprocess
from desc.twinkles import get_visits

logging.basicConfig()

def coadd_id(band):
    return 'filter=%s patch=0,0 tract=0' % band

def ingestImages(phosim_dir, image_repo, pattern='lsst_*.fits.gz', logger=None):
    """
    Run obs_lsstSim/bin/ingestImages.py in order to set up the initial
    image repository for the phosim data.
    """
    if logger is None:
        logger = logging.getLogger()
    command = 'ingestImages.py %(phosim_dir)s %(phosim_dir)s/%(pattern)s --mode link --output %(image_repo)s --doraise --clobber-config' % locals()
    logger.info("running:\n  " + command)
    subprocess.call(command, shell=True)

def getVisits(image_repo):
    """
    Extract the visit per band info from the registry.sqlite3 db in
    the image repository and repackage the lists of visits for feeding
    directly to the Level 2 pipe tasks.
    """
    visits = get_visits(image_repo)
    visits = OrderedDict([(band, '^'.join([str(x) for x in vals]))
                          for band, vals in visits.items()])
    return visits

class Level2_Pipeline(object):
    """
    Class to manage Level 2 pipeline execution run serially on a data repo.
    """
    def __init__(self, image_repo, output_repo, visits,
                 pipe_task_options="--doraise --clobber-config",
                 logger_level=logging.INFO):
        """
        image_repo: directory with ingested phosim images
        output_repo: directory where pipe task results will be put
        visits: dictionary of visits keyed by filter.
        """
        self.image_repo = image_repo
        self.output_repo = output_repo
        self.visits = visits
        self.pipe_task_options = pipe_task_options
        self.logger = logging.getLogger()
        self.logger.setLevel(logger_level)
        self.failures = OrderedDict()
        self.all_visits = '^'.join(visits.values())
        self._generate_methods()

    def run(self, dry_run=False):
        "Run the full pipeline."
        self.run_processEimage(dry_run=dry_run)
        self.run_makeDiscreteSkyMap(dry_run=dry_run)
        self.run_makeCoaddTempExp(dry_run=dry_run)
        self.run_assembleCoadd(dry_run=dry_run)
        self.run_detectCoaddSources(dry_run=dry_run)
        self.run_mergeCoaddDetections(dry_run=dry_run)
        self.run_measureCoaddSources(dry_run=dry_run)
        self.run_mergeCoaddMeasurements(dry_run=dry_run)
        self.run_forcedPhotCcd(dry_run=dry_run)

    def _generate_methods(self):
        for pipe_task in 'detectCoaddSources mergeCoaddDetections measureCoaddSources mergeCoaddMeasurements'.split():
            self._generate_method(pipe_task)

    def _generate_method(self, pipe_task):
        def run_pipe_task(self, dry_run=False):
            output = self.output_repo
            failures = OrderedDict()
            my_coadd_id = coadd_id('^'.join('ugrizy'))
            pipe_task_options = self.pipe_task_options
            command = pipe_task + '.py %(output)s/ --id %(my_coadd_id)s --output %(output)s %(pipe_task_options)s' % locals()
            self.logger.info('running:\n  ' + command)
            if dry_run:
                return
            try:
                subprocess.check_call(command, shell=True)
            except subprocess.CalledProcessError as eobj:
                failures[pipe_task] = {my_coadd_id : eobj}
        run_pipe_task.__doc__ = "Run %s.py" % pipe_task
        run_pipe_task.__name__ = 'run_%s' % pipe_task
        setattr(self.__class__, run_pipe_task.__name__, run_pipe_task)

    def run_processEimage(self, dry_run=False):
        "Run processEimage.py on all of the visits."
        failures = OrderedDict()
        for visit in self.all_visits.split('^'):
            image_repo = self.image_repo
            output_repo = self.output_repo
            options = self.pipe_task_options
            command = 'processEimage.py %(image_repo)s/ --id visit=%(visit)s --output %(output_repo)s %(options)s' % locals()
            self.logger.info('running:\n  ' + command)
            if dry_run:
               continue
            try:
                subprocess.check_call(command, shell=True)
            except subprocess.CalledProcessError as eobj:
                failures[visit] = eobj
        if failures:
            self.failures['processEimage'] = failures

    def run_makeDiscreteSkyMap(self, dry_run=False):
        "Run makeDiscreteSkyMap.py on all visits"
        output = self.output_repo
        all_visits = self.all_visits
        pipe_task_options = self.pipe_task_options
        command = "makeDiscreteSkyMap.py %(output)s/ --id visit=%(all_visits)s --output %(output)s %(pipe_task_options)s" % locals()
        self.logger.info('running:\n  ' + command)
        if dry_run:
            return
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError as eobj:
            self.failures['makeDiscreteSkyMap'] = {all_visits : eobs}

    def run_makeCoaddTempExp(self, dry_run):
        "Run makeCoaddTempExp.py on all visits."
        output = self.output_repo
        pipe_task_options = self.pipe_task_options
        failures = OrderedDict()
        for filt in 'ugrizy':
            visits = self.visits[filt]
            command = ('makeCoaddTempExp.py %(output)s/ --selectId visit=%(visits)s --id '
                       + coadd_id(filt)
                       + ' --config bgSubtracted=True --output %(output)s %(pipe_task_options)s') % locals()
            self.logger.info('running:\n  ' + command)
            if dry_run:
                continue
            try:
                subprocess.check_call(command, shell=True)
            except subprocess.CalledProcessError as eobj:
                failures[visits] = eobj
        if failures:
            self.failures['makeCoaddTempExp'] = failures

    def run_assembleCoadd(self, dry_run):
        "assemble coadds"
        output = self.output_repo
        pipe_task_options = self.pipe_task_options
        failures = OrderedDict()
        for filt in 'ugrizy':
            visits = self.visits[filt]
            command = ('assembleCoadd.py %(output)s/ --selectId visit=%(visits)s --id '
                       + coadd_id(filt)
                       + ' --config doInterp=True --output %(output)s %(pipe_task_options)s') % locals()
            self.logger.info('running:\n  ' + command)
            if dry_run:
                continue
            try:
                subprocess.check_call(command, shell=True)
            except subprocess.CalledProcessError as eobj:
                failures[visits] = eobj
        if failures:
            self.failures['assembleCoadd'] = failures

    def run_forcedPhotCcd(self, dry_run=False, raft='2,2', sensor='1,1'):
        "Run forcedPhotCcd.py"
        output = self.output_repo
        visits = self.all_visits
        pipe_task_options = self.pipe_task_options
        command = "forcedPhotCcd.py %(output)s/ --id tract=0 visit=%(visits)s sensor=%(sensor)s raft=%(raft)s --config measurement.doApplyApCorr='yes' --output %(output)s %(pipe_task_options)s" % locals()
        self.logger.info('running:\n  ' + command)
        if dry_run:
            return
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError as eobj:
            failures['forcedPhotCcd'] = {visits : eobj}

if __name__ == '__main__':
    phosim_dir = 'images'
    image_repo = 'image_repo'
    output_repo = 'output_repo'

    ingestImages(phosim_dir, image_repo)
    visits = getVisits(image_repo)
    l2 = Level2_Pipeline(image_repo, output_repo, visits)
    l2.run()

    for task, failures in l2.failures.items():
        print task + " had %i failed executions:" % len(failures)
        for visit, eobj in failures.items():
            print visit, eobj
