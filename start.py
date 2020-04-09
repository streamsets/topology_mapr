# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import re
import tempfile
import time
import yaml
from socket import gethostbyname, gethostname, socket

from clusterdock.models import Cluster, client, Node
from clusterdock.utils import version_tuple, wait_for_condition
from javaproperties import PropertiesFile

from . import st

DEFAULT_NAMESPACE = 'clusterdock'
DEFAULT_SDC_REPO = 'https://s3-us-west-2.amazonaws.com/archives.streamsets.com/datacollector/'
DEFAULT_STF_GROUP_ID = '20149'
DEFAULT_STF_GROUP_NAME = 'stf'
DEFAULT_STF_USER_ID = '20149'
DEFAULT_STF_USER_NAME = 'stf'
DEFAULT_STF_USER_PASSWORD = 'stf'
EARLIEST_MAPR_VERSION_WITH_LICENSE_AND_CENTOS_7 = (6, 0, 0)
EARLIEST_MAPR_VERSION_WITH_HIVE_META_SERVICE = (6, 1, 0)
# For MEP 4.0 onwards, MAPR_MEP_VERSION env. variable is needed by setup_mapr script.
EARLIEST_MEP_VERSION_FOR_SETUP_MAPR_SCRIPT = (4, 0)
MAPR_CONFIG_DIR = '/opt/mapr/conf'
MAPR_SERVERTICKET_FILE = 'maprserverticket'
MAPR_USERTICKET_FILE = 'mapruserticket'
MCS_SERVER_PORT = 8443
SECURE_CONFIG_CONTAINER_DIR = '/etc/clusterdock/secure'
SSL_KEYSTORE_FILE = 'ssl_keystore'
SSL_TRUSTSTORE_FILE = 'ssl_truststore'

SECURE_FILES = [
    MAPR_SERVERTICKET_FILE,
    SSL_KEYSTORE_FILE,
    SSL_TRUSTSTORE_FILE
]

# Spark home and hadoop home needed to setup transformer.  These are symlinks created by
# the topology. The actual install of spark and hadoop on a mapr cluster would be
# of the form /opt/mapr/spark/spark-{version}/ and /opt/mapr/hadoop/hadoop-{version}/
SPARK_HOME = '/opt/mapr/spark/spark'
HADOOP_HOME = '/opt/mapr/hadoop/hadoop'

logger = logging.getLogger('clusterdock.{}'.format(__name__))


def main(args):
    quiet = not args.verbose
    if args.license_url and not args.license_credentials:
        raise Exception('--license-credentials is a required argument if --license-url is provided.')

    image_prefix = '{}/{}/clusterdock:mapr{}'.format(args.registry,
                                                     args.namespace or DEFAULT_NAMESPACE,
                                                     args.mapr_version)
    if args.mep_version:
        image_prefix = '{}_mep{}'.format(image_prefix, args.mep_version)
    primary_node_image = '{}_{}'.format(image_prefix, 'primary-node')
    secondary_node_image = '{}_{}'.format(image_prefix, 'secondary-node')

    node_disks = yaml.load(args.node_disks)

    # MapR-FS needs each fileserver node to have a disk allocated for it, so fail fast if the
    # node disks map is missing any nodes.
    if set(args.primary_node + args.secondary_nodes) != set(node_disks):
        raise Exception('Not all nodes are accounted for in the --node-disks dictionary')

    # Docker for Mac exposes ports that can be accessed only with ``localhost:<port>`` so
    # use that instead of the hostname if the host name is ``moby``.
    if any(docker_for_mac_name in client.info().get('Name', '') for docker_for_mac_name in ['moby', 'linuxkit']):
        hostname = 'localhost'
    else:
        hostname = gethostbyname(gethostname())

    primary_node = Node(hostname=args.primary_node[0],
                        group='primary',
                        image=primary_node_image,
                        ports=[{MCS_SERVER_PORT: MCS_SERVER_PORT}
                               if args.predictable
                               else MCS_SERVER_PORT],
                        devices=node_disks.get(args.primary_node[0]),
                        # Secure cluster needs the ticket to execute rest of commands
                        # after cluster start.
                        environment=({'MAPR_TICKETFILE_LOCATION': os.path.join(MAPR_CONFIG_DIR,
                                                                               MAPR_USERTICKET_FILE)}
                                     if args.secure else {}))

    secondary_nodes = [Node(hostname=hostname,
                            group='secondary',
                            image=secondary_node_image,
                            devices=node_disks.get(hostname),
                            # Secure cluster needs the ticket to execute rest of commands
                            # after cluster start.
                            environment=({'MAPR_TICKETFILE_LOCATION': os.path.join(MAPR_CONFIG_DIR,
                                                                                   MAPR_USERTICKET_FILE)}
                                         if args.secure else {}))
                       for hostname in args.secondary_nodes]

    # If transformer is specified, add it to the cluster.
    transformer = None
    if args.st_version:
        transformer = st.Transformer(args.st_version, args.namespace or 'streamsets', args.registry,
                                     args.st_resources_directory,
                                     args.sch_server_url, args.sch_username, args.sch_password)
        logger.debug('Adding transformer ports to primary node ...')
        primary_node.ports.append({st.ST_PORT: st.ST_PORT} if args.predictable else st.ST_PORT)

        logger.debug('Adding Transformer image %s to primary node ...', transformer.image_name)
        primary_node.volumes.append(transformer.image_name)

        logger.debug('Adding Transformer extra lib images to primary node ...')
        primary_node.volumes.extend(transformer.extra_lib_images)

        if args.st_resources_directory:
            logger.debug('Volume mounting Transformer resources from %s to %s ...',
                         transformer.st_resources_directory_path, transformer.st_resources_mount_point)
            primary_node.volumes.append({transformer.st_resources_directory_path: transformer.st_resources_mount_point})

        #Set up transformer related environment variables in the primary node.
        transformer.environment['SPARK_HOME'] = SPARK_HOME
        transformer.environment['HADOOP_CONF_DIR'] = '{}/etc/hadoop'.format(HADOOP_HOME)
        primary_node.environment.update(transformer.environment)

    cluster = Cluster(primary_node, *secondary_nodes)

    if args.secure:
        secure_config_host_dir = os.path.expanduser(args.secure_config_directory)
        volumes = [{secure_config_host_dir: SECURE_CONFIG_CONTAINER_DIR}]
        for node in cluster.nodes:
            node.volumes.extend(volumes)

    # MapR versions 6.0.0 onwards use CentOS 7 which needs following settings.
    mapr_version_tuple = tuple(int(i) for i in args.mapr_version.split('.'))
    if mapr_version_tuple >= EARLIEST_MAPR_VERSION_WITH_LICENSE_AND_CENTOS_7:
        for node in cluster.nodes:
            node.volumes.append({'/sys/fs/cgroup': '/sys/fs/cgroup'})
            temp_dir_name = tempfile.mkdtemp()
            logger.debug('Created temporary directory %s', temp_dir_name)
            node.volumes.append({temp_dir_name: '/run'})
    cluster.primary_node = primary_node
    cluster.start(args.network, pull_images=args.always_pull)

    logger.info('Generating new UUIDs ...')
    cluster.execute('/opt/mapr/server/mruuidgen > /opt/mapr/hostid', quiet=quiet)

    if not args.secure:
        logger.info('Configuring the cluster ...')
        for node in cluster:
            configure_command = ('/opt/mapr/server/configure.sh -C {0} -Z {0} -RM {0} -HS {0} '
                                 '-u mapr -g mapr -D {1}'.format(
                                     primary_node.fqdn,
                                     ','.join(node_disks.get(node.hostname))
                                 ))
            node.execute("bash -c '{}'".format(configure_command), quiet=quiet)
    else:
        logger.info('Configuring native security for the cluster ...')
        configure_command = ('/opt/mapr/server/configure.sh -secure -genkeys -C {0} -Z {0} -RM {0} -HS {0} '
                             '-u mapr -g mapr -D {1}'.format(
                                 primary_node.fqdn,
                                 ','.join(node_disks.get(primary_node.hostname))
                             ))
        source_files = ['{}/{}'.format(MAPR_CONFIG_DIR, file) for file in SECURE_FILES]
        commands = [configure_command,
                    'chmod 600 {}/{}'.format(MAPR_CONFIG_DIR, SSL_KEYSTORE_FILE),
                    'cp -f {src} {dest_dir}'.format(src=' '.join(source_files),
                                                    dest_dir=SECURE_CONFIG_CONTAINER_DIR)]
        primary_node.execute(' && '.join(commands), quiet=quiet)
        for node in secondary_nodes:
            source_files = ['{}/{}'.format(SECURE_CONFIG_CONTAINER_DIR, file)
                            for file in SECURE_FILES]
            configure_command = ('/opt/mapr/server/configure.sh -secure -C {0} -Z {0} -RM {0} -HS {0} '
                                 '-u mapr -g mapr -D {1}'.format(
                                     primary_node.fqdn,
                                     ','.join(node_disks.get(node.hostname))
                                 ))
            commands = ['cp -f {src} {dest_dir}'.format(src=' '.join(source_files),
                                                        dest_dir=MAPR_CONFIG_DIR),
                        configure_command]
            node.execute(' && '.join(commands), quiet=quiet)

    logger.info('Waiting for MapR Control System server to come online ...')

    def condition(address, port):
        return socket().connect_ex((address, port)) == 0

    def success(time):
        logger.info('MapR Control System server is online after %s seconds.', time)

    def failure(timeout):
        raise TimeoutError('Timed out after {} seconds waiting '
                           'for MapR Control System server to come online.'.format(timeout))
    wait_for_condition(condition=condition,
                       condition_args=[primary_node.ip_address, MCS_SERVER_PORT],
                       time_between_checks=3, timeout=180, success=success, failure=failure)
    mcs_server_host_port = primary_node.host_ports.get(MCS_SERVER_PORT)

    # If this is a secure cluster, then generate a ticket for the mapr user on each node of the
    # cluster.
    if args.secure:
        for node in cluster.nodes:
            node.execute('maprlogin password <<< "mapr"', user='mapr', quiet=quiet)

        # Restart the hiveserver once a new ticket has been created.  HiveServer2 is configured only on the primary
        # node.  When the hiveserver starts initially, the metastore schema creation fails because it is missing the
        # user ticket.  This user ticket is created when the previous command is run.
        hive_version_str = primary_node.execute('basename `echo /opt/mapr/hive/hive-*`',
                                                quiet=True).output.strip().split('-')[1]
        hive_version_home = '/opt/mapr/hive/hive-{}'.format(hive_version_str)
        primary_node.execute(
            'maprcli node services -name {0} -action stop -nodes {1}'.format(
                'hivemeta' if mapr_version_tuple >= EARLIEST_MAPR_VERSION_WITH_HIVE_META_SERVICE else 'hs2',
                primary_node.fqdn), user='mapr', quiet=quiet)

        def hiveserver_stopped_condition(node, service):
            status = int(node.execute(
                "maprcli service list -output terse -node {0} | grep {1} | awk '{{print $1}}'".format(node.fqdn,
                                                                                                      service),
                quiet=quiet).output.strip() or -1)
            # check if service status is either stopped (3) or not configured (0).
            return status in (0, 3)

        wait_for_condition(condition=hiveserver_stopped_condition,
                           condition_args=[primary_node,
                                           'hivemeta'
                                           if mapr_version_tuple >= EARLIEST_MAPR_VERSION_WITH_HIVE_META_SERVICE
                                           else 'hs2'],
                           timeout=30)

        primary_node.execute('rm -rf {}/bin/metastore_db'.format(hive_version_home), user='mapr', quiet=quiet)
        primary_node.execute('cd {}/bin/ && ./schematool -dbType derby -initSchema'.format(hive_version_home),
                             user='mapr', quiet=quiet)

        # Restart both hive meta store and the hiveserver2 which depends on hive metastore. Depending on the
        # version of MapR hive metastore service may or may not be installed.
        if mapr_version_tuple >= EARLIEST_MAPR_VERSION_WITH_HIVE_META_SERVICE:
            primary_node.execute(
                'maprcli node services -name hivemeta -action start -nodes {}'.format(primary_node.fqdn), user='mapr',
                quiet=quiet)
        primary_node.execute('maprcli node services -name hs2 -action restart -nodes {}'.format(primary_node.fqdn),
                             user='mapr', quiet=quiet)

        # In a secure mode deployment, change the permissions so that any user can write to the hive.warehouse.dir.
        # This is because, we impersonate user 'stf' when running tests and so that directory needs to writable by stf.
        primary_node.execute(' && '.join(['hadoop fs -mkdir -p /user/hive/warehouse',
                                          'hadoop fs -chmod -R 777 /user/hive/warehouse']),
                             user='mapr', quiet=quiet)

    # Wait for Node Manager configuration to be complete before attempting any other configuration changes.  The only
    # good way to do this is to look for the presence of a directory in hadoop fs.  The presence of this directory
    # indicates that the configuration is complete.
    def mapr_configuration_completed(node):
        logger.debug('Waiting for MapR configuration to be completed')
        return node.execute('hadoop fs -ls /var/mapr/local/{}/mapred/nodeManager/output.U'.format(node.fqdn),
                            quiet=quiet).exit_code == 0

    wait_for_condition(mapr_configuration_completed, condition_args=[primary_node], timeout=240)

    logger.info('Creating /apps/spark directory on %s ...', primary_node.hostname)
    spark_directory_command = ['hadoop fs -mkdir -p /apps/spark',
                               'hadoop fs -chmod 777 /apps/spark']
    primary_node.execute("bash -c '{}'".format('; '.join(spark_directory_command)), user='mapr', quiet=quiet)

    logger.info('Creating MapR sample Stream named /sample-stream on %s ...', primary_node.hostname)
    primary_node.execute('maprcli stream create -path /sample-stream '
                         '-produceperm p -consumeperm p -topicperm p', user='mapr', quiet=quiet)

    # Determine the spark version
    sparkversion_cmd = primary_node.execute('basename `echo /opt/mapr/spark/spark-*`', quiet=True)
    spark_version_str = sparkversion_cmd.output.strip().split('-')[1]
    spark_version_home = '/opt/mapr/spark/spark-{}'.format(spark_version_str)

    # Determine the hadoop version
    hadoopversion_cmd = primary_node.execute('cat /opt/mapr/hadoop/hadoopversion', quiet=True)
    hadoop_version_home = '/opt/mapr/hadoop/hadoop-{}'.format(hadoopversion_cmd.output.strip())

    # Starting with MapR 6.1.0, a separate hive metastore service is installed. Modify & copy
    # the hive-site.xml into spark config directory, for spark-sql to work with hive.
    hive_site_xml_contents = ''
    if mapr_version_tuple >= EARLIEST_MAPR_VERSION_WITH_HIVE_META_SERVICE:
        # Determine the hive version
        hiveversion_cmd = primary_node.execute('basename `echo /opt/mapr/hive/hive-*`', quiet=True)
        hive_version_str = hiveversion_cmd.output.strip().split('-')[1]
        hive_version_home = '/opt/mapr/hive/hive-{}'.format(hive_version_str)

        # Read the contents of the hive-site.xml. This will be copied over to the spark conf directory.
        hive_site_xml_contents = primary_node.get_file('{}/conf/hive-site.xml'.format(hive_version_home))
        hive_site_xml_contents = re.sub('localhost', primary_node.fqdn, hive_site_xml_contents)
        hive_site_xml_config_string = ('<property>\n'
                                       '   <name>hive.exec.scratchdir</name>\n'
                                       '   <value>/tmp</value>\n'
                                       '</property>\n'
                                       '<property>\n'
                                       '   <name>hive.exec.local.scratchdir</name>\n'
                                       '   <value>/tmp</value>\n'
                                       '</property>\n')
        hive_site_xml_contents = re.sub('(</configuration>)', '{}\\1'.format(hive_site_xml_config_string),
                                        hive_site_xml_contents)

    # Configure the dynamic allocation for MapR
    spark_defaults_config_file = '{}/conf/spark-defaults.conf'.format(spark_version_home)
    spark_dynamic_allocation_config = {'spark.dynamicAllocation.enabled': 'true',
                                       'spark.dynamicAllocation.minExecutors': '5',
                                       'spark.executor.instances': '1',
                                       'spark.shuffle.service.enabled': 'true'}

    # Configure core-site.xml so that impersonation works properly.
    core_site_config_file = '{}/etc/hadoop/core-site.xml'.format(hadoop_version_home)
    core_site_config_string = ('<property>\n'
                               '    <name>hadoop.proxyuser.{transformer_user}.hosts</name>\n'
                               '    <value>*</value>\n'
                               '</property>\n'
                               '<property>\n'
                               '    <name>hadoop.proxyuser.{transformer_user}.groups</name>\n'
                               '    <value>*</value>\n'
                               '</property>\n'
                               '<property>\n'
                               '    <name>hadoop.proxyuser.{stf_user}.hosts</name>\n'
                               '    <value>*</value>\n'
                               '</property>\n'
                               '<property>\n'
                               '    <name>hadoop.proxyuser.{stf_user}.groups</name>\n'
                               '    <value>*</value>\n'
                               '</property>\n'
                               '<property>\n'
                               '    <name>hadoop.proxyuser.{sdc_user}.hosts</name>\n'
                               '    <value>*</value>\n'
                               '</property>\n'
                               '<property>\n'
                               '    <name>hadoop.proxyuser.{sdc_user}.groups</name>\n'
                               '    <value>*</value>\n'
                               '</property>\n'
                               '<property>\n'
                               '    <name>fs.mapr.server.resolve.user</name>\n'
                               '    <value>true</value>\n'
                               '</property>\n'.format(transformer_user=st.ST_USER,
                                                      stf_user=DEFAULT_STF_USER_NAME,
                                                      sdc_user='sdc'))

    # Configure the yarn-site.xml file for external shuffle service
    yarn_site_config_file = '{}/etc/hadoop/yarn-site.xml'.format(hadoop_version_home)
    yarn_config_string = ('<property>\n'
                          '    <name>yarn.nodemanager.aux-services</name>\n'
                          '    <value>mapreduce_shuffle,mapr_direct_shuffle,spark_shuffle</value>\n'
                          '</property>\n'
                          '<property>\n'
                          '    <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>\n'
                          '    <value>org.apache.spark.network.yarn.YarnShuffleService</value>\n'
                          '</property>\n')
    if args.secure:
        yarn_config_string += ('<property>\n'
                               '    <name>spark.authenticate</name>\n'
                               '    <value>true</value>\n'
                               '</property>\n')

    # The location of the shuffle jar depends on the spark version.
    spark_shuffle_jar = 'yarn/spark-*-mapr-*-yarn-shuffle.jar'
    if version_tuple(spark_version_str) <= version_tuple('1.6.1'):
        spark_shuffle_jar = 'lib/spark-*-mapr-*-yarn-shuffle.jar'

    spark_config_commands = '; '.join(
        ['cp -p {}/{} {}/share/hadoop/yarn/lib/'.format(spark_version_home, spark_shuffle_jar, hadoop_version_home),
         'ln -s {} {}'.format(spark_version_home, SPARK_HOME),
         'ln -s {} {}'.format(hadoop_version_home, HADOOP_HOME)]
    )
    for node in cluster.nodes:
        # Create a user that is not the mapr user in each node.  This user will be impersonated by transformer tests,
        # since mapr has a limitation that the mapr user cannot be impersonated.
        create_stf_user = ('groupadd -g {gid} {group} && '
                           'useradd -g {gid} -u {uid} {user} && '
                           'echo {password} | passwd {user} --stdin'.format(gid=DEFAULT_STF_GROUP_ID,
                                                                            group=DEFAULT_STF_GROUP_NAME,
                                                                            uid=DEFAULT_STF_USER_ID,
                                                                            user=DEFAULT_STF_USER_NAME,
                                                                            password=DEFAULT_STF_USER_PASSWORD))
        node.execute(create_stf_user, quiet=quiet)

        # Update the spark-defaults.conf file on each node.
        spark_default_config = PropertiesFile.loads(node.get_file(spark_defaults_config_file))
        spark_default_config.update(spark_dynamic_allocation_config)
        node.put_file(spark_defaults_config_file, PropertiesFile.dumps(spark_default_config))

        # Put the hive-site.xml into spark config directory.
        node.put_file('{}/conf/hive-site.xml'.format(spark_version_home), hive_site_xml_contents)

        # Update the core-site.xml config on each node
        core_site_config = node.get_file(core_site_config_file)
        node.put_file(core_site_config_file,
                      re.sub('(</configuration>)', '{}\\1'.format(core_site_config_string), core_site_config))

        # On each node, create files in the /opt/mapr/conf/proxy directory to support
        # impersonation.  For impersonation on MapR, the transfomer user or the sdc user can impersonate.
        node.execute('touch /opt/mapr/conf/proxy/{}'.format(st.ST_USER), quiet=quiet)
        node.execute('touch /opt/mapr/conf/proxy/{}'.format('sdc'), quiet=quiet)
        node.execute('touch /opt/mapr/conf/proxy/{}'.format(DEFAULT_STF_USER_NAME), quiet=quiet)

        # Update the yarn-site.xml config on each node
        yarn_site_config = node.get_file(yarn_site_config_file)
        node.put_file(yarn_site_config_file,
                      re.sub('(</configuration>)', '{}\\1'.format(yarn_config_string), yarn_site_config))

        node.execute(spark_config_commands, quiet=quiet)

    # Set the cldb.security.resolve.user property to 1.  This is necessary for impersonation to work properly
    primary_node.execute('maprcli config save -values {cldb.security.resolve.user:1}', quiet=quiet)

    # We need to wait for the cldb service to start before proceeding. the cldb service is configured only on the
    # primary node.
    primary_node.execute('maprcli node services -name cldb -action restart -nodes {}'.format(primary_node.fqdn),
                         quiet=quiet)

    def cldb_started_condition(node):
        cldb_status = int(node.execute(
            "maprcli service list -output terse -node {0} | grep cldb | awk '{{print $1}}'".format(node.fqdn),
            quiet=quiet).output.strip() or -1)
        # Check to see if the service status is either 0 (not configured on the node) or 2 (running).
        return cldb_status in (0, 2)

    wait_for_condition(condition=cldb_started_condition, condition_args=[primary_node], timeout=120)

    # Restart services whose configuration was modified on the nodes.  This is needed so that the config changes made
    # above can take affect. If the service is not configured on the node it is a no-op.
    nodes = ' '.join([node.fqdn for node in cluster.nodes])
    primary_node.execute('maprcli node services -name nodemanager -action restart -nodes {}'.format(nodes), quiet=quiet)
    primary_node.execute('maprcli node services -name resourcemanager -action restart -nodes {}'.format(nodes),
                         quiet=quiet)

    if mapr_version_tuple >= EARLIEST_MAPR_VERSION_WITH_LICENSE_AND_CENTOS_7 and args.license_url:
        license_commands = ['curl --user {} {} > /tmp/lic'.format(args.license_credentials,
                                                                  args.license_url),
                            '/opt/mapr/bin/maprcli license add -license /tmp/lic -is_file true',
                            'rm -rf /tmp/lic']
        logger.info('Applying license ...')
        primary_node.execute(' && '.join(license_commands), quiet=quiet)

    if not args.dont_register_gateway:
        logger.info('Registering gateway with the cluster ...')
        register_gateway_commands = ["cat /opt/mapr/conf/mapr-clusters.conf | egrep -o '^[^ ]* '"
                                     ' > /tmp/cluster-name',
                                     'maprcli cluster gateway set -dstcluster $(cat '
                                     '/tmp/cluster-name) -gateways {}'.format(primary_node.fqdn),
                                     'rm /tmp/cluster-name']
        primary_node.execute(' && '.join(register_gateway_commands), quiet=quiet)

    logger.info('Creating sdc and stf user directories in MapR-FS ...')
    create_user_directory_command = ['hadoop fs -mkdir -p /user/sdc',
                                     'hadoop fs -chown sdc:sdc /user/sdc',
                                     'hadoop fs -mkdir /user/{}'.format(DEFAULT_STF_USER_NAME),
                                     'hadoop fs -chown {0}:{0} /user/{0}'.format(DEFAULT_STF_USER_NAME)]
    primary_node.execute('; '.join(create_user_directory_command), user='mapr', quiet=quiet)

    if args.sdc_version:
        logger.info('Installing StreamSets DataCollector version %s ...', args.sdc_version)
        _install_streamsets_datacollector(primary_node, args.sdc_version,
                                          args.mapr_version, args.mep_version, args.verbose)
        logger.info('StreamSets DataCollector version %s is installed using rpm. '
                    'Install additional stage libraries using rpm ...', args.sdc_version)

    # start transformer if it is configured
    if transformer:
        logger.debug('Adding user %s for Transformer ...', st.ST_USER)
        create_user_command = ' && '.join(transformer.commands_for_add_user())
        for node in cluster.nodes:
            node.execute(create_user_command, quiet=quiet)

        logger.debug('Creating Transformer specific directories ...')
        for path in transformer.directories_to_create():
            primary_node.execute('mkdir -p {}'.format(path), quiet=quiet)
            primary_node.execute('chown -R {user}:{user} {path}'.format(user=st.ST_USER, path=path))

        logger.info('Creating transformer user directory in MapR-FS ...')
        create_transformer_user_directory_command = ['hadoop fs -mkdir -p /user/{}'.format(st.ST_USER),
                                                     'hadoop fs -chown {0}:{0} /user/{0}'.format(st.ST_USER)]
        primary_node.execute('; '.join(create_transformer_user_directory_command), user='mapr', quiet=quiet)

        if args.sch_server_url:
            logger.info('Enabling StremSets Control Hub for Transformer ...')
            transformer.sch_login()
            sch_comp_auth_token = transformer.sch_create_components()
            transformer_sch_app_token_file_path = os.path.join(transformer.environment['TRANSFORMER_CONF'],
                                                               st.SCH_APPLICATION_TOKEN_FILE_NAME)
            primary_node.put_file(transformer_sch_app_token_file_path, sch_comp_auth_token)

            transformer_sch_properties_file_path = os.path.join(transformer.environment['TRANSFORMER_CONF'],
                                                                st.SCH_PROPERTIES_FILE_NAME)
            transformer_sch_properties_file = primary_node.get_file(transformer_sch_properties_file_path)
            transformer_sch_properties = PropertiesFile.loads(transformer_sch_properties_file)
            transformer_sch_property_values = {
                'dpm.enabled': 'true',
                'dpm.base.url': args.sch_server_url
            }
            transformer_sch_properties.update(transformer_sch_property_values)
            primary_node.put_file(transformer_sch_properties_file_path,
                                  PropertiesFile.dumps(transformer_sch_properties))

        logger.info('Running Transformer as user %s from %s ...', st.ST_USER, transformer.home_dir)
        command_for_execute = transformer.command_for_execute()
        if args.secure:
            # If this is a secure MapR cluster, create a service ticket for use with the transformer process
            transformer_service_ticket_location = '{}/maprservice_ticket'.format(transformer.home_dir)
            primary_node.execute('maprlogin generateticket -type servicewithimpersonation -out {} -duration 30:0:0 '
                                 '-renewal 90:0:0 -user {}'.format(transformer_service_ticket_location, st.ST_USER),
                                 quiet=quiet)
            primary_node.execute('chown {0}:{0} {1}'.format(st.ST_USER, transformer_service_ticket_location),
                                 quiet=quiet)
            command_for_execute = 'export MAPR_TICKETFILE_LOCATION={} && {}'.format(transformer_service_ticket_location,
                                                                                    command_for_execute)
        primary_node.execute(command_for_execute, user=st.ST_USER, detach=True, quiet=quiet)

        def st_condition(command):
            return primary_node.execute(command, quiet=quiet).exit_code == 0
        wait_for_condition(condition=st_condition, timeout=120,
                           condition_args=['ss -ntl | grep ":{}"'.format(st.ST_PORT)])
        st_http_url = 'http://{}:{}'.format(hostname, primary_node.host_ports.get(st.ST_PORT))
        logger.info('Transformer is now reachable at %s', st_http_url)

    logger.info('MapR Control System server is now accessible at https://%s:%s',
                hostname, mcs_server_host_port)


# Returns wget commands and rpm package names for all the rpm packages needed.
# These lists depend on the SDC version, MapR version and MEP version.
def _gather_wget_commands_and_rpm_names(whole_sdc_version, mapr_version, mep_version):
    result_rpms = []
    sdc_version = whole_sdc_version.rsplit('-RC')[0]
    sdc_rpm = 'streamsets-datacollector-{}-1.noarch.rpm'.format(sdc_version)
    result_rpms.append(sdc_rpm)
    name = 'streamsets-datacollector-mapr_{mapr_version}-lib-{sdc_version}-1.noarch.rpm'
    sdc_mapr_rpm = name.format(sdc_version=sdc_version,
                               mapr_version='_'.join(mapr_version.split('.')[:2]))
    result_rpms.append(sdc_mapr_rpm)

    result_wgets = []
    el_part = ''
    if sdc_version.startswith('3'):
        el_part = 'el7/' if mapr_version.startswith('6') else 'el6/'
    base_url = '{sdc_repo}{sdc_ver}/rpm/{el_part}'.format(sdc_repo=DEFAULT_SDC_REPO,
                                                          sdc_ver=whole_sdc_version,
                                                          el_part=el_part)

    # RPM for SDC.
    result_wgets.append('wget -q {base_url}{sdc_rpm}'.format(base_url=base_url, sdc_rpm=sdc_rpm))
    # RPM for MapR stage library.
    result_wgets.append('wget -q {base_url}{sdc_mapr_rpm}'.format(base_url=base_url,
                                                                  sdc_mapr_rpm=sdc_mapr_rpm))
    # RPM for MEP stage library for MapR 6.
    if mapr_version.startswith('6'):
        mep_rpm_name = 'streamsets-datacollector-mapr_{}-mep{}-lib-{}-1.noarch.rpm'
        mep_rpm = mep_rpm_name.format('_'.join(mapr_version.split('.')[:2]),
                                      mep_version[:1],
                                      sdc_version)
        result_wgets.append('wget -q {base_url}{mep_rpm}'.format(base_url=base_url,
                                                                 mep_rpm=mep_rpm))
        result_rpms.append(mep_rpm)
    # RPM for Spark 2.1 stage library for MapR 5.2.2.
    if mapr_version == '5.2.2' and mep_version.startswith('3'):
        spark_rpm_name = 'streamsets-datacollector-mapr_spark_2_1_mep_{}-lib-{}-1.noarch.rpm'
        spark_rpm = spark_rpm_name.format('_'.join(mep_version.split('.')[:2]), sdc_version)
        result_wgets.append('wget -q {base_url}{spark_rpm}'.format(base_url=base_url,
                                                                   spark_rpm=spark_rpm))
        result_rpms.append(spark_rpm)

    return result_wgets, result_rpms


# Installation of SDC happens in following major steps:
# Fetch and install all the rpm packages for the SDC core and MapR stage-libs
# Run the script to setup MapR
# Start SDC service
def _install_streamsets_datacollector(primary_node, sdc_version, mapr_version, mep_version, verbose):
    quiet = not verbose
    primary_node.execute('JAVA_HOME=/usr/java/jdk1.8.0_131', quiet=quiet)
    primary_node.execute('cd /opt/')
    wget_commands, rpm_names = _gather_wget_commands_and_rpm_names(sdc_version, mapr_version, mep_version)
    primary_node.execute('; '.join(wget_commands), quiet=quiet)  # Fetch all rpm packages
    primary_node.execute('yum -y -q localinstall {}'.format(' '.join(rpm_names)), quiet=quiet)
    is_mapr_6 = mapr_version.startswith('6')
    # For MEP 4.0 onwards, MAPR_MEP_VERSION env. variable is needed by setup_mapr script.
    # And for earlier MEP versions than 4.0, the script takes no effect if that env. variable is set up.
    mapr_mep_version = ''
    if mep_version:
        mep_version_tuple = tuple(int(i) for i in mep_version.split('.'))
        if mep_version_tuple >= EARLIEST_MEP_VERSION_FOR_SETUP_MAPR_SCRIPT:
            mapr_mep_version = 'MAPR_MEP_VERSION={}'.format(mep_version[:1])
    setup_mapr_cmd = (' SDC_HOME=/opt/streamsets-datacollector SDC_CONF=/etc/sdc MAPR_HOME=/opt/mapr'
                      ' MAPR_VERSION={mapr_version} {mapr_mep_version}'
                      ' /opt/streamsets-datacollector/bin/streamsets setup-mapr >& /tmp/setup-mapr.out')
    primary_node.execute(setup_mapr_cmd.format(mapr_version=mapr_version[:5],
                                               mapr_mep_version=mapr_mep_version), quiet=quiet)
    primary_node.execute('rm -f {}'.format(' '.join(rpm_names)), quiet=quiet)
    if is_mapr_6:
        primary_node.execute('systemctl start sdc; systemctl enable sdc', quiet=quiet)
    else:
        primary_node.execute('service sdc start; chkconfig --add sdc', quiet=quiet)
