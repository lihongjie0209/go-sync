FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates curl bzip2 zlib1g-dev libreadline-dev flex bison pkg-config libicu-dev && rm -rf /var/lib/apt/lists/*
ARG PG_VERSION=9.4.26
RUN curl -fsSL https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2 | tar -xj -C /tmp && \
    cd /tmp/postgresql-${PG_VERSION} && ./configure --prefix=/opt/pg --without-readline --without-icu && make -j4 && make install && rm -rf /tmp/postgresql-${PG_VERSION}
ENV PATH=/opt/pg/bin:$PATH
RUN curl -fsSL https://github.com/eulerto/wal2json/archive/refs/tags/wal2json_2_6.tar.gz | tar -xz -C /tmp && \
    cd /tmp/wal2json-wal2json_2_6 && make && make install && rm -rf /tmp/wal2json-wal2json_2_6
RUN curl -fsSL https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2 | tar -xj -C /tmp && \
    make -C /tmp/postgresql-${PG_VERSION}/contrib/pg_trgm USE_PGXS=1 install && rm -rf /tmp/postgresql-${PG_VERSION}
RUN useradd -m postgres && mkdir /data && chown postgres /data
USER postgres
RUN initdb -D /data --no-locale --encoding=UTF8 --auth=trust
COPY --chown=postgres:postgres pg_hba.conf /data/pg_hba.conf
COPY entrypoint.sh /entrypoint.sh
EXPOSE 5432
CMD ["sh","/entrypoint.sh"]
