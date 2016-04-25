# inspired by / copied from https://github.com/nkratzke/EasyMySQL

FROM broadinstitute/rawls:dev-base

# NOTE: our Google Cloud SQL uses MySQL version 5.6.29, possibly with some Google customizations
# the closest available apt-get version is a moving target: 5.6.28 at script creation time, 5.6.30 currently

# Install MySQL
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    mysql-server-5.6 mysql-client-5.6 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Setup test user
ENV user rawls-test
ENV password rawls-test
COPY ./setup-test-user.sh /usr/local/bin/setup-test-user.sh
RUN chmod +x /usr/local/bin/setup-test-user.sh
RUN /usr/local/bin/setup-test-user.sh

# Start MySQL when the container starts, and don't start Rawls
RUN mkdir /etc/service/mysql
RUN ln -s /usr/sbin/mysqld /etc/service/mysql/run
RUN rm /etc/service/rawls/run
