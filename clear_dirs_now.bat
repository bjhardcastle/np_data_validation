@REM if necessary, clone repo with:
@REM git clone https://github.com/AllenInstitute/np_data_validation.git
git checkout main
git pull origin main

@REM if environment doesn't exist, create it with:
@REM conda create --file environment.yml
ECHO off
title Checking for valid backups and clearing local directories

git checkout main
git pull origin main

CALL conda env create --file environment.yml
CALL conda activate dv
CALL pip install -r requirements.txt

CALL python data_validation.py
cmd \k