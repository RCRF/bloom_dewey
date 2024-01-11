
PGDATA=${PGDATA:-bloom_lims/database/bloom_lims} 
PGUSER=${PGUSER:-$USER}
PGPASSWORD=${PGPASSWORD:-passw0rd}
export PGDBNAME=${PGDBNAME:-bloom}
export PGPORT=5432

pg_ctl -D $PGDATA -o "-p $PGPORT"  -l $PGDATA/db.log start 