FROM us.gcr.io/broad-dsp-gcr-public/base/jre:17-debian

# To run, must build the jar using ./docker/build.sh

# Rawls' default port
EXPOSE 8080
EXPOSE 5050

ENV GIT_MODEL_HASH $GIT_MODEL_HASH

RUN mkdir /rawls
COPY ./rawls*.jar /rawls

# Add Rawls as a service (it will start when the container starts)
# 1. “Exec” form of CMD necessary to avoid “shell” form’s `sh` stripping 
#    environment variables with periods in them, often used in DSP for Lightbend 
#    config.
# 2. Handling $JAVA_OPTS is necessary as long as firecloud-develop or the app’s 
#    chart tries to set it. Apps that use devops’s foundation subchart don’t need 
#    to handle this.
# 3. The jar’s location and naming scheme in the filesystem is required by preflight 
#    liquibase migrations in some app charts. Apps that expose liveness endpoints 
#    may not need preflight liquibase migrations.
# We use the “exec” form with `bash` to accomplish all of the above.
CMD ["/bin/bash", "-c", "java $JAVA_OPTS -jar $(find /rawls -name 'rawls*.jar')"]

# These next 4 commands are for enabling SSH to the container.
# id_rsa.pub is referenced below, but this should be any public key
# that you want to be added to authorized_keys for the root user.
# Copy the public key into this directory because ADD cannot reference
# Files outside of this directory

#EXPOSE 22
#RUN rm -f /etc/service/sshd/down
#ADD id_rsa.pub /tmp/id_rsa.pub
#RUN cat /tmp/id_rsa.pub >> /root/.ssh/authorized_keys
