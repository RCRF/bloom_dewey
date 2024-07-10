echo "\n\n___STOPPING POSTGRES___\n\n"
sleep 1
source bloom_lims/bin/stop_bloom_db.sh 

echo "\n\n___REMOVING DATABASE DIRS & FILES___\n\n"
setopt rmstarsilent
rm -rf bloom_lims/database/*
unsetopt rmstarsilent
sleep 1

echo "\n\n___BUILDING POSTGRES && SEEDING WITH TEMPLATES___\n\n"
sleep 1
source bloom_lims/env/install_postgres.sh skip

echo "\n\n___RUNNING TESTS___\n\n"
sleep 1
pytest 