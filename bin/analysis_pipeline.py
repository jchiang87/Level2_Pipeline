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
from collections import OrderedDict
import subprocess
from desc.twinkles import get_visits

input = 'input_data'
output = 'output_data'
sensor = '1,1'
raft = '2,2'
coadd_id = lambda filt : 'filter=%s patch=0,0 tract=0' % filt

#command = 'ingestImages.py images images/lsst_*.fits.gz --mode link --output %(input)s --doraise --clobber-config' % locals()
#print command
#subprocess.call(command, shell=True)

visits = get_visits(input)
visits = OrderedDict([(band, '^'.join([str(x) for x in vals]))
                      for band, vals in visits.items()])
print visits
all_visits = '^'.join(visits.values())
print all_visits

command_templates = []
#for visit in all_visits.split('^'):
#    command_templates.append(
#        'processEimage.py %(input)s/ --id visit=%(visit)s --output %(output)s --doraise --clobber-config' % locals()
#        )
#
#command_templates.append('makeDiscreteSkyMap.py %(output)s/ --id visit=%(all_visits)s --output %(output)s --doraise --clobber-config')
#
#for filt in 'ugrizy':
#    visit = visits[filt]
#    command_templates.append(
#        ('makeCoaddTempExp.py %(output)s/ --selectId visit=%(visit)s --id '
#         + coadd_id(filt)
#         + ' --config bgSubtracted=True --output %(output)s --doraise --clobber-config')
#        % locals())
#
#for filt in 'ugrizy':
#    visit = visits[filt]
#    command_templates.append(
#        ('assembleCoadd.py %(output)s/ --selectId visit=%(visit)s --id '
#         + coadd_id(filt)
#         + ' --config doInterp=True --output %(output)s --doraise --clobber-config')
#        % locals())

my_coadd_id = coadd_id('^'.join('ugrizy'))
command_templates.extend(
    [
#    'detectCoaddSources.py %(output)s/ --id %(my_coadd_id)s --output %(output)s --doraise --clobber-config',
#    'mergeCoaddDetections.py %(output)s/ --id %(my_coadd_id)s --output %(output)s --doraise --clobber-config',
    'measureCoaddSources.py %(output)s/ --id %(my_coadd_id)s --output %(output)s --doraise --clobber-config',
    'mergeCoaddMeasurements.py %(output)s/ --id %(my_coadd_id)s --output %(output)s --doraise --clobber-config',
    "forcedPhotCcd.py %(output)s/ --id tract=0 visit=%(all_visits)s sensor=%(sensor)s raft=%(raft)s --config measurement.doApplyApCorr='yes' --output %(output)s --doraise --clobber-config"
    ]
    )

for item in command_templates:
    command = item % locals()
    print command
    print
    sys.stdout.flush()
    subprocess.call(command, shell=True)
