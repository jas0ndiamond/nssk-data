FROM mysql:8.0-debian

ENV TZ="America/Vancouver"

MAINTAINER jason.a.diamond@gmail.com

RUN apt-get update && apt-get install -y logrotate fail2ban less procps && apt-get clean all && rm -rf /var/lib/apt/lists/*

ADD ./setup/0_create_dbs.sql /docker-entrypoint-initdb.d
ADD ./setup/1_create_users.sql /docker-entrypoint-initdb.d
ADD ./setup/2_create_nssk_cosmo_tables.sql /docker-entrypoint-initdb.d
ADD ./setup/3_create_cnv_rainfall_tables.sql /docker-entrypoint-initdb.d
ADD ./setup/4_create_flowworks_tables.sql /docker-entrypoint-initdb.d

# fail2ban config for mysql
COPY ./fail2ban/jail.local /etc/fail2ban/jail.local
COPY ./fail2ban/fail2ban.conf /etc/fail2ban/fail2ban.conf

# won't work until mysql logs are created on fs
#RUN service fail2ban start

RUN mkdir /var/log/mysql
RUN chown mysql:mysql /var/log/mysql

EXPOSE 3306
