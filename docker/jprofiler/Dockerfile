FROM broadinstitute/rawls:dev-base

RUN wget http://download-aws.ej-technologies.com/jprofiler/jprofiler_linux_9_1.tar.gz && tar -xvf jprofiler_linux_9_1.tar.gz && rm jprofiler_linux_9_1.tar.gz

ADD run.sh /etc/service/rawls/run
