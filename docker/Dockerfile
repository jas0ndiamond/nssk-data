FROM mysql:8.0-debian

ENV TZ="America/Vancouver"

MAINTAINER jason.a.diamond@gmail.com

RUN apt-get update && apt-get install -y logrotate fail2ban less procps && apt-get clean all && rm -rf /var/lib/apt/lists/*

# database setup scripts
ADD ./database_setup/0_create_dbs.sql /docker-entrypoint-initdb.d
ADD ./database_setup/1_create_users.sql /docker-entrypoint-initdb.d
ADD ./database_setup/2_create_nssk_cosmo_tables.sql /docker-entrypoint-initdb.d
ADD ./database_setup/3_create_cnv_rainfall_tables.sql /docker-entrypoint-initdb.d
ADD ./database_setup/4_create_dnv_whitewater_tables.sql /docker-entrypoint-initdb.d
ADD ./database_setup/5_create_conductivity_rainfall_correlation_tables.sql /docker-entrypoint-initdb.d
ADD ./database_setup/mysql.txt /

# fail2ban config for mysql
COPY ./fail2ban/jail.local /etc/fail2ban/jail.local
COPY ./fail2ban/fail2ban.conf /etc/fail2ban/fail2ban.conf

# won't work until mysql logs are created on fs
#RUN service fail2ban start

RUN mkdir /var/log/mysql
RUN chown mysql:mysql /var/log/mysql

# database port
EXPOSE 3306

# healthcheck - container typically takes a minute to fully startup
# TODO - proper implementation with mysqladmin_ping and auth
HEALTHCHECK --interval=60s --timeout=5s --retries=5 CMD timeout 1 bash -c '</dev/tcp/127.0.0.1/3306' || exit 1
