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

import hashlib
import json
import logging
import os

import docker
import requests
from clusterdock.utils import join_url_parts

logger = logging.getLogger('clusterdock.{}'.format(__name__))

EXTRA_LIB_IMAGE_NAME_TEMPLATE = '{}/{}/transformer:{}'
IMAGE_NAME_TEMPLATE = '{}/{}/transformer:{}'

EXTRA_LIBS_SUPPORTED = ['transformer-jdbc']
ST_FORM_REALM_PROPERTIES_FILENAME = 'form-realm.properties'
ST_PORT = 19630 # inline to ST build
ST_PROPERTIES_FILE_NAME = 'transformer.properties'
ST_USER = 'transformer'
ST_USER_ID = 20169 # inline to ST build

DEFAULT_SCH_API_VERSION = 1
SCH_APPLICATION_TOKEN_FILE_NAME = 'application-token.txt'
SCH_PROPERTIES_FILE_NAME = 'dpm.properties'

docker_client = docker.from_env(timeout=300)


class Transformer:
    def __init__(self, version, namespace, registry, resources_directory=None,
                 sch_server_url=None, sch_username=None, sch_password=None):
        self.version = version
        self.numeric_version, self.version_specifier = ((version, None) if not '-' in version
                                                        else version.split('-', maxsplit=1))
        self.image_name = IMAGE_NAME_TEMPLATE.format(registry, namespace, self.version)
        self.extra_lib_images = [EXTRA_LIB_IMAGE_NAME_TEMPLATE.format(registry, namespace, image)
                                 for image in EXTRA_LIBS_SUPPORTED]

        self.sch_server_url = sch_server_url
        self.sch_username = sch_username
        self.sch_password = sch_password
        self.sch_organization = self.sch_username.split('@')[1] if self.sch_username else None

        logger.info('Attempting to pull newest Transformer image (%s) ...', self.image_name)
        docker_client.images.pull(self.image_name)

        container = docker_client.api.create_container(image=self.image_name)['Id']
        raw_env_vars = docker_client.api.inspect_container(container).get('Config').get('Env')
        env_vars = {key: value for key, value in (env_var.split('=') for env_var in raw_env_vars)}
        self.home_dir = env_vars['TRANSFORMER_HOME'] # typically: /opt/streamsets-transformer-{version}
        self.resources_directory = env_vars.get('TRANSFORMER_RESOURCES') # typically: '/resources/st'
        self.environment = {
            'JAVA_HOME': env_vars.get('JAVA_HOME'), # typically: '/opt/java/openjdk'
            'TRANSFORMER_CONF': env_vars.get('TRANSFORMER_CONF'), # typically: '/etc/st'
            'TRANSFORMER_DATA': env_vars.get('TRANSFORMER_DATA'), # typically: '/data/st'
            'TRANSFORMER_DIST': self.home_dir,
            'TRANSFORMER_HOME': self.home_dir,
            'TRANSFORMER_LOG': env_vars.get('TRANSFORMER_LOG'), # typically: '/logs/st'
            'TRANSFORMER_RESOURCES': self.resources_directory,
            # typically: '/opt/streamsets-libs-extras'
            'STREAMSETS_LIBRARIES_EXTRA_DIR': env_vars.get('STREAMSETS_LIBRARIES_EXTRA_DIR'),
            # typically: '/opt/streamsets-transformer-user-libs'
            'USER_LIBRARIES_DIR': env_vars.get('USER_LIBRARIES_DIR')
        }
        docker_client.api.remove_container(container=container, v=True, force=True)

        if resources_directory:
            # When an external resources directory is passed in, then map as a sub directory under the
            # resource directory specified in the container.  For example, if a directory called
            # /tmp/transformer-resources is passed in and the resource directory volume exposed by the transformer
            # container is /resources/transformer, then set the mount point to
            # /resources/transformer/transformer-resources.
            self.st_resources_directory_path = os.path.realpath(os.path.expanduser(resources_directory))
            self.st_resources_mount_point = os.path.join(self.resources_directory,
                                                         os.path.basename(self.st_resources_directory_path))

        if self.sch_server_url:
            self.sch_session = requests.Session()
            self.sch_session.headers.update({'X-Requested-By': 'dpm',
                                            'X-SS-REST-CALL': 'true',
                                            'content-type': 'application/json'})
            self.sch_session.verify = True

    def directories_to_create(self):
        return [self.environment['TRANSFORMER_DATA']]

    def commands_for_add_user(self):
        return ['groupadd -r -g {} {}'.format(ST_USER_ID, ST_USER),
                'useradd -r -u {} -g {} {}'.format(ST_USER_ID, ST_USER, ST_USER)]

    def command_for_execute(self):
        return '{}/bin/streamsets transformer -exec'.format(self.home_dir)

    def add_user_util(self, form_realm_properties, user, password=None, roles=None, groups=None):
        """A utility method to add a user to Transformer form_realm_properties.

        Args:
            form_realm_properties (:obj:`dict`): Provide a form realm properties to update.
            user (:obj:`str`): User to add.
            password (:obj:`str`, optional): Password for user. Default: same as ``user``
            roles (:obj:`list`, optional): List of roles to assign to user. Default: ``None``
            groups (:obj:`list`, optional): List of groups to make user a member of.
                Default: ``None``
        """
        # If password isn't set, make it the same as the user name to follow existing conventions.
        password = 'MD5:{0}'.format(hashlib.md5((password or user).encode()).hexdigest())
        roles = ','.join(roles or [])
        groups = ','.join('group:{0}'.format(group) for group in groups or [])

        # 'user' must always be present between password and roles, so we add it.
        user_properties_value = ','.join(section
                                         for section in (password, 'user', roles, groups)
                                         if section)
        logger.debug('Adding %s: %s to form_realm_properties ...', user, user_properties_value)
        form_realm_properties[user] = user_properties_value

    def sch_login(self):
        response = self._sch_post(app='security',
                                  endpoint='/v{}/authentication/login'.format(DEFAULT_SCH_API_VERSION),
                                  rest='public-rest',
                                  data={'userName': self.sch_username, 'password': self.sch_password})
        self.sch_session.headers.update({'X-SS-User-Auth-Token': response.cookies.get('SS-SSO-LOGIN')})

    def sch_create_components(self):
        data = {
            'organization': self.sch_organization,
            'componentType': 'transformer',
            'numberOfComponents': 1,
            'active': True
        }
        response = self._sch_put(app='security',
                                 endpoint='/v{}/organization/{}/components'.format(DEFAULT_SCH_API_VERSION,
                                                                                   self.sch_organization),
                                 data=data)
        full_auth_token = response.json()[0]['fullAuthToken']
        return full_auth_token

    # Internal SCH functions only below.
    def _sch_delete(self, app, endpoint, rest='rest', params=None):
        url = join_url_parts(self.sch_server_url, app, '/{}'.format(rest), endpoint)
        response = self.sch_session.delete(url, params=params or {})
        self._handle_http_error(response)
        return response

    def _sch_get(self, app, endpoint, rest='rest', params=None):
        url = join_url_parts(self.sch_server_url, app, '/{}'.format(rest), endpoint)
        response = self.sch_session.get(url, params=params or {})
        self._handle_http_error(response)
        return response

    def _sch_post(self, app, endpoint, rest='rest', params=None, data=None, files=None, headers=None):
        url = join_url_parts(self.sch_server_url, app, '/{}'.format(rest), endpoint)
        if not data:
            data = None
        else:
            data = data if isinstance(data, str) else json.dumps(data)
        response = self.sch_session.post(url, params=params or {}, data=data, files=files, headers=headers)
        self._handle_http_error(response)
        return response

    def _sch_put(self, app, endpoint, rest='rest', params=None, data=None):
        url = join_url_parts(self.sch_server_url, app, '/{}'.format(rest), endpoint)
        response = self.sch_session.put(url, params=params or {}, data=json.dumps(data or {}))
        self._handle_http_error(response)
        return response

    def _handle_http_error(self, response):
        # Delegating to response object error handling as last resort.
        try:
            response.raise_for_status()
        except:
            logger.error('Encountered an error while doing %s on %s. Response: %s',
                         response.request.method,
                         response.request.url,
                         response.__dict__)
            raise
