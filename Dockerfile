# http://phusion.github.io/baseimage-docker/
FROM phusion/baseimage

# Rawls' default port
EXPOSE 8080
EXPOSE 5050

# Use baseimage's init system.
CMD ["/sbin/my_init"]

# Install Rawls
RUN apt-get update && apt-get install -y wget && \
    add-apt-repository "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc) multiverse" && \
    add-apt-repository -y ppa:webupd8team/java && \
    echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections && \
    echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections && \
    apt-get update && \
    apt-get install -y oracle-java8-installer && \
    wget http://www.scala-lang.org/files/archive/scala-2.11.4.deb && \
    wget http://dl.bintray.com/sbt/debian/sbt-0.13.7.deb && \
    dpkg -i scala-2.11.4.deb && \
    dpkg -i sbt-0.13.7.deb && \
    apt-get update && \
    apt-get install -y scala sbt && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /*.deb

ADD . /rawls
RUN ["/bin/bash", "-c", "/rawls/docker/install.sh /rawls"]

# Add Rawls as a service (it will start when the container starts)
RUN mkdir /etc/service/rawls
ADD docker/run.sh /etc/service/rawls/run

# These next 4 commands are for enabling SSH to the container.
# id_rsa.pub is referenced below, but this should be any public key
# that you want to be added to authorized_keys for the root user.
# Copy the public key into this directory because ADD cannot reference
# Files outside of this directory

#EXPOSE 22
#RUN rm -f /etc/service/sshd/down
#ADD id_rsa.pub /tmp/id_rsa.pub
#RUN cat /tmp/id_rsa.pub >> /root/.ssh/authorized_keys
