ECHO off
title Checking for valid backups and clearing local directories

git checkout main
git pull origin main

CALL conda env create --file environment.yml
CALL conda activate dv

CALL python data_validation.py
cmd \k