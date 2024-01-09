
PGDATA=${PGDATA:-bloom_lims/database/bloom_lims} 
PGUSER=${PGUSER:-$USER}
PGPASSWORD=${PGPASSWORD:-passw0rd}
export PGDBNAME=${PGDBNAME:-bloom}

pg_ctl -D $PGDATA  -l $PGDATA/db.log stop 