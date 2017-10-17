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
    mapr-core \
    mapr-fileserver \
    mapr-hbase-regionserver \
    mapr-nodemanager
