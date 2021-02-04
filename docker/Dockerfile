FROM docker.cicd-jfrog.telkomsel.co.id/rhel7.3:7.3

RUN mkdir -p /usr/src/app /var/log/app /etc/app /data /root/.pip
WORKDIR /usr/src/app
VOLUME /var/log/app /etc/app /data

COPY src/requirements.txt /usr/src/app
COPY docker/artifactory.repo /etc/yum.repos.d/
COPY docker/files/pip.conf /root/.pip/pip.conf

ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

RUN mkdir -p /usr/share/man/man1 \
    && yum -y update \
    && yum install -y java-1.8.0-openjdk-devel gcc gcc-c++ make python3 python3-devel \
    && yum install -y postgresql-devel \
    && yum clean all \
    && pip3 install --upgrade --no-cache-dir setuptools \
    && pip3 install --no-cache-dir pip==20.0.2 \
    && pip3 install --no-cache-dir pypandoc==1.4 \
    && pip3 install --no-cache-dir cx-Oracle==7.3.0 \
    && pip3 install --no-cache-dir -r requirements.txt \
    && pip3 install --no-cache-dir -r requirements.txt

# Install GDAL & Fiona
#RUN pip3 install https://cicd-jfrog.telkomsel.co.id/artifactory/api/pypi/tsel-python-local/GDAL-3.1.3-cp36-cp36m-win_amd64.whl
#RUN pip3 install https://cicd-jfrog.telkomsel.co.id/artifactory/api/pypi/tsel-python-local/Fiona-1.8.17-cp36-cp36m-win_amd64.whl

RUN yum install -y unzip wget git
RUN wget -c https://cicd-jfrog.telkomsel.co.id/artifactory/public-local/instantclient-basic-linux.x64-19.6.0.0.0dbru.zip --no-check-certificate
RUN unzip instantclient-basic-linux.x64-19.6.0.0.0dbru.zip -d /root/lib

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV GIT_PYTHON_REFRESH quiet
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8
ENV PATH="/root/.local/bin:${PATH}"
ENV LD_LIBRARY_PATH="/root/lib"

RUN ln -s /opt/cloudera/parcels/CDH/bin/hdfs /usr/local/bin/hdfs

ARG HADOOP_VERSION="2.6.0-cdh5.13.3"
ENV HADOOP_HOME "/opt/cloudera/parcels/CDH-5.13.3-1.cdh5.13.3.p3182.3440/lib/hadoop"
ENV HADOOP_COMMON_HOME "${HADOOP_HOME}"
ENV HADOOP_CONF_DIR "${HADOOP_HOME}/etc/hadoop"
ENV PATH "$PATH:${HADOOP_HOME}/bin"
ENV HADOOP_COMMON_LIB_NATIVE_DIR "${HADOOP_HOME}/lib/native"

RUN yum install -y krb5-workstation krb5-libs krb5-auth-dialog libaio
COPY docker/files/krb5.conf /etc/

CMD [ "/bin/bash" ]
