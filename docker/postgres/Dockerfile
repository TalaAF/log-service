# Postgres with pg_cron, used for retention (see 0002_retention_pg_cron.sql).
#
# pg_cron is a background worker, so it must be present in
# shared_preload_libraries before the server starts — that cannot be done from
# a compose `environment:` block or by CREATE EXTENSION alone, which is why
# this image exists rather than using postgres:16-alpine directly.
#
# Debian-based rather than Alpine: pg_cron is packaged for PGDG (apt) but has
# no ready Alpine package, which would otherwise mean compiling from source.
FROM postgres:16

# Kept in sync with POSTGRES_DB in docker-compose.yml via a build arg, so the
# scheduler database and the application database cannot silently diverge.
ARG POSTGRES_DB=logs

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-cron \
    && rm -rf /var/lib/apt/lists/*

# postgresql.conf.sample is the template initdb copies into PGDATA on first
# boot, so appending here applies the settings to a freshly initialised volume.
#
# cron.database_name must be the application database: pg_cron installs into
# exactly one database per cluster and cron.schedule() cannot reach across
# databases. It defaults to "postgres", which would put the scheduler somewhere
# it cannot see the logs table.
RUN echo "shared_preload_libraries = 'pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample \
    && echo "cron.database_name = '${POSTGRES_DB}'" >> /usr/share/postgresql/postgresql.conf.sample
