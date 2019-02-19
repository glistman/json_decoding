FROM postgres:11
MAINTAINER glistman

RUN apt-get update
RUN apt-get install -f -y --no-install-recommends \
        software-properties-common \
        build-essential \
        pkg-config \
        git \
        postgresql-server-dev-11 \
        libproj-dev \
        postgresql-11-postgis-2.5 \
        postgresql-11-postgis-2.5-scripts \
        postgis \
        liblwgeom-dev

RUN rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/glistman/json_decoding -b master --single-branch \
    && cd /json_decoding \
    && make && make install \
    && cd / \
    && rm -rf json_decoding

COPY postgres.conf.sample /usr/share/postgresql/postgresql.conf.sample
RUN echo "host replication $POSTGRES_USER 0.0.0.0/0 trust" >> "$PGDATA/pg_hba.conf"