FROM postgres:10

MAINTAINER glistman

RUN apt-get update \
    && apt-get install -f -y --no-install-recommends \
        software-properties-common \
        build-essential \
        pkg-config \
        git \
        postgresql-server-dev-10 \
        libproj-dev \
        postgresql-10-postgis-2.4 \
        postgresql-10-postgis-2.4-scripts \
        postgis \
        liblwgeom-dev

RUN rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/glistman/json_decoding -b p10 --single-branch \
    && cd /json_decoding \
    && make USE_PGXS=1 && make install \
    && cd / \
    && rm -rf json_decoding

COPY postgres.conf.sample /usr/share/postgresql/postgresql.conf.sample
RUN echo "host replication $POSTGRES_USER 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf"