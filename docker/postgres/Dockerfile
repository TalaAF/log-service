# Postgres with pg_cron, used for retention (see 0002_retention_pg_cron.sql)
# and for topping up weekly partitions (see 0003_write_path.sql).
#
# This image exists only to install the extension binary. pg_cron is a
# background worker, so it must be listed in shared_preload_libraries before the
# server starts — that setting, along with the rest of the server tuning, is
# passed on the postgres command line from docker-compose.yml, which has the
# advantage of applying to an already-initialised data volume as well as to a
# fresh one. Appending to postgresql.conf.sample, as this image used to do, only
# takes effect at initdb time.
#
# Debian-based rather than Alpine: pg_cron is packaged for PGDG (apt) but has
# no ready Alpine package, which would otherwise mean compiling from source.
#
# pg_stat_statements needs no package: it ships with postgresql-contrib in the
# official image and only has to be preloaded.
FROM postgres:16

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-cron \
    && rm -rf /var/lib/apt/lists/*
