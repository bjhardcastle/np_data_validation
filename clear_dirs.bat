@REM if necessary, clone repo with:
@REM git clone https://github.com/AllenInstitute/np_data_validation.git
git checkout main
git pull origin main

@REM if environment doesn't exist, create it with:
@REM conda create --file environment.yml
CALL conda activate dv

python data_validation.py
