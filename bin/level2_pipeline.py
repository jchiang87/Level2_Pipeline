#!/usr/bin/env python
"""
This needs to be run in a bash environment where
$ setup obs_lsstSim
$ setup -m none -r and_files astrometry_net_data
have been run and which contains an images/ and and_files/
subdirectories.
"""
from desc.level2_pipeline import ingest_images, get_visits, Level2_Pipeline

phosim_dir = 'images'
image_repo = 'image_repo'
output_repo = 'output_repo'

ingest_images(phosim_dir, image_repo)
visits = get_visits(image_repo)
l2 = Level2_Pipeline(image_repo, output_repo, visits)
l2.run(dry_run=False)
l2.report_failures()
