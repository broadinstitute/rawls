FROM broadinstitute/rawls:dev

RUN wget http://download-aws.ej-technologies.com/jprofiler/jprofiler_linux_9_2.tar.gz && tar -xvf jprofiler_linux_9_2.tar.gz && rm jprofiler_linux_9_2.tar.gz

ADD run.sh /etc/service/rawls/run

CMD bash /etc/service/rawls/run
