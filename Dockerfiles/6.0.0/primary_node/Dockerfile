FROM clusterdock/topology_nodebase:centos6.6

# Build arguments.
ARG REPO_USERNAME
ARG REPO_PASSWORD

ADD mapr_core.repo /tmp/mapr_core.repo
ADD mapr_mep.repo /tmp/mapr_mep.repo
RUN echo "baseurl = http://${REPO_USERNAME}:${REPO_PASSWORD}@stage.mapr.com/beta/v6.0.0/v6.0.0/redhat/" >> /tmp/mapr_core.repo
RUN echo "baseurl = http://${REPO_USERNAME}:${REPO_PASSWORD}@stage.mapr.com/beta/v6.0.0/MEP/MEP-4.0.0/redhat/" >> /tmp/mapr_mep.repo
RUN cp /tmp/mapr_core.repo /etc/yum.repos.d/
RUN cp /tmp/mapr_mep.repo /etc/yum.repos.d/

RUN rpm --import http://package.mapr.com/releases/pub/maprgpg.key

RUN yum -y install java-1.8.0-openjdk-devel \
    mapr-cldb \
    mapr-core \
    mapr-fileserver \
    mapr-gateway \
    mapr-hbase \
    mapr-hbasethrift \
    mapr-hbase-master \
    mapr-hbase-regionserver \
    mapr-hbase-rest \
    mapr-historyserver \
    mapr-hive \
    mapr-hiveserver2 \
    mapr-httpfs \
    mapr-kafka \
    mapr-nodemanager \
    mapr-resourcemanager \
    mapr-spark \
    mapr-webserver \
    mapr-zookeeper \
    mysql-server
