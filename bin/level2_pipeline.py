#!/usr/bin/env python
"""
This needs to be run in a bash environment where
$ setup obs_lsstSim
$ setup -m none -r and_files astrometry_net_data
have been run and which contains an images/ and and_files/
subdirectories.
"""
from desc.level2_pipeline import ingestImages, Level2_Pipeline

phosim_dir = 'images'
image_repo = 'image_repo'
output_repo = 'output_repo'

ingestImages(phosim_dir, image_repo)
visits = getVisits(image_repo)
l2 = Level2_Pipeline(image_repo, output_repo, visits)
l2.run(dry_run=False)

for task, failures in l2.failures.items():
    print task + " had %i failed executions:" % len(failures)
    for visit, eobj in failures.items():
        print visit, eobj
