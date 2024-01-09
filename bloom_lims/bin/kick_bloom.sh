
skip_conda=$1

source bloom_lims/bin/stop_bloom.sh
rm -rf bloom_lims/database/*
source bloom_lims/env/install_postgres.sh $skip_conda
ls ./bloom_lims/config/*/*json | parallel 'python seed_db_containers.py {}' 
pytest