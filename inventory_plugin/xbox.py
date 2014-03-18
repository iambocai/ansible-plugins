#!/bin/env python
# -*- encoding: utf8 -*-
"""
Xbox external inventory script
=================================

Ansible has a feature where instead of reading from /etc/ansible/hosts
as a text file, it can query external programs to obtain the list
of hosts, groups the hosts are in, and even variables to assign to each host.

To use this, copy this file over /etc/ansible/hosts and chmod +x the file.
This, more or less, allows you to keep one central database containing
info about all of your managed instances.

This script is an example of sourcing that data from Xbox
(http://git.n.xiaomi.com/).  With xbox each --mgmt-class in xbox
will correspond to a group in Ansible, and --ks-meta variables will be
passed down for use in templates or even in argument lines.

NOTE: The xbox system names will not be used.  Make sure a
xbox --dns-name is set for each xbox system.   If a system
appears with two DNS names we do not add it twice because we don't want
ansible talking to it twice.  The first one found will be used. If no
--dns-name is set the system will NOT be visible to ansible.  We do
not add xbox system names because there is no requirement in xbox
that those correspond to addresses.

See http://ansible.github.com/api.html for more info

Tested with Xbox 2.0.11.

Changelog:
    - 2013-09-01 pgehres: Refactored implementation to make use of caching and to
        limit the number of connections to external xbox server for performance.
        Added use of xbox.ini file to configure settings. Tested with Xbox 2.4.0
"""

# (c) 2012, Michael DeHaan <michael.dehaan@gmail.com>
#
# This file is part of Ansible,
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

######################################################################

import sys
from argparse import ArgumentParser
import ConfigParser
import os
import re
from time import time
import logging
import urllib
import httplib2
import socket

import simplejson as json
from ansible import utils
from ansible import callbacks
from ansible import errors

# NOTE -- this file assumes Ansible is being accessed FROM the xbox
# server, so it does not attempt to login with a username and password.
# this will be addressed in a future version of this script.


class XboxInventory(object):

    def __init__(self):
        """ Main execution path """

        self.cache = dict()
        self.inventory = dict()  # A list of groups and the hosts in that group

        # Read settings and parse CLI arguments
        self.read_settings()
        self.parse_cli_args()

        # Cache
        if self.args.refresh_cache:
            self.update_cache()
        elif not self.is_cache_valid():
            self.update_cache()
        else:
            self.load_inventory_from_cache()

        data_to_print = ""

        # Data to print
        if self.args.host:
            data_to_print = self.get_host_info()

        elif self.args.list:
            # Display list of instances for inventory
            data_to_print = self.json_format_dict(self.inventory, True)

        else:  # default action with no options
            data_to_print = self.json_format_dict(self.inventory, True)

        print data_to_print

    def get_host_list(self):
        '''根据用户配置从xbox获取机器列表'''

        http = httplib2.Http()
        headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'User-Agent': 'ansible-host-getter'}
        url = self.xbox_url_get_hostlist

        for group in self.xbox_groups:
            body = {'token': self.xbox_token, 'tag':group }
            url = self.xbox_url_get_hostlist + urllib.urlencode(body)

            try:
                response, content = http.request(url, 'GET', headers=headers)
                # 检查返回是否正常
                if response.status != 200:
                    raise AnsibleXboxResponseError('%s %s' % (response.status,response.reason))

                hosts = json.loads(content)['hosts']
                callbacks.display('[DEBUG] [get_host_list] url[%s] ==> hosts[%s]' % (url, len(hosts)), log_only=True)
                for host in hosts:
                    self.push(self.inventory, 'all', host)
            # 连不上?
            except httplib2.HttpLib2Error, e:
                callbacks.display('[ERROR] [get_host_list] Connect to xbox failed','red')
                sys.exit(1)
            # http返回非200?
            except AnsibleXboxResponseError, e:
                callbacks.display('[ERROR] [get_host_list] Xbox response error, http status: %s' % e,'red')
                sys.exit(1)
            # 解析数据出错了?
            except KeyError or json.ValueError, e:
                callbacks.display('[ERROR] [get_host_list] Decode response error, maybe xbox api changed' ,'red')
                logging.exception("")
                sys.exit(1)
            # 其他可能
            except Exception, e:
                callbacks.display('[ERROR] [get_host_list] Unknown error: %s' % e ,'red')
                logging.exception("MARK_ANSIBLE_NEED_REVIEW")
                sys.exit(1)

    def get_host_tags(self):
        '''根据机器列表获得所有机器的tag和ip信息，并将tag作为分组名插入inventory'''

        # 根据机器名获取tag的api支持批量查询，但为了避免超过http get size，需要将查询切成几次
        all_item_num  = len(self.inventory['all']) 
        item_per_req  = 200
        # 查询次数 = 上取整（全部机器数/每次查询包含的机器数）
        req_group_num = int((item_per_req+all_item_num-1)/item_per_req)
        
        for group in range(1, req_group_num+1 ):
            # 每次从全量机器列表中截 item_per_req 个机器，合成xbox规定的string进行请求
            idx_left  = (group -1) * item_per_req
            idx_right = min( group * item_per_req, all_item_num)
            callbacks.display( \
                '[DEBUG] [get_host_tags] all[%d] per[%d] group[%d/%d] idx[%d:%d)' % \
                 (all_item_num, item_per_req, group,req_group_num,idx_left, idx_right), \
                 log_only=True \
            )
            host_list = '_'.join(self.inventory['all'][idx_left:idx_right])
            

            http = httplib2.Http()
            headers = {
                'Content-type': 'application/x-www-form-urlencoded',
                'User-Agent': 'ansible-host-getter'}

            body = {'token': self.xbox_token, 'hosts': host_list }
            url = self.xbox_url_get_hosttags + urllib.urlencode(body)
            try:
                response, content = http.request(url, 'GET', headers=headers)
                # 检查返回是否正常
                if response.status not in (200, 304):
                    raise AnsibleXboxResponseError('%s %s' % (response.status,response.reason))
                data = json.loads(content)
                callbacks.display('[DEBUG] [get_host_tags] url[%s] ==> hosts[%s]' % (url, len(data['tag_list'])), log_only=True)

                # 返回结果失败或返回空taglist
                if data['succ'] !=0 or not data['tag_list']:
                    raise AnsibleXboxQueryError("note:[%s] url:[%s]" %(data[err_note],url))
                # 查询返回的机器数少于查询时发出的机器数,请求可能被截断了
                if len(data['tag_list']) != idx_right - idx_left:
                    callbacks.display('[WARN] Host num in response lower than query sent, some host may lost', 'yallow')
                
                for host in json.loads(content)['tag_list'].keys():
                    self.cache[host] = dict()
                    ip = 'Unknown'
                    try: 
                        ip = socket.gethostbyname(host)
                    except:
                        pass
                    self.cache[host]['ip'] = [ip]
                    tagstrs = json.loads(content)['tag_list'][host].split(',')
                    for tagstr in tagstrs:
                        for tag in tagstr.split('_'):
                            [key, value] = tag.split('.')
                            # 将tag作为hostvar保存
                            self.push(self.cache[host], key, value)
                            # 也将tag作为分组依据
                            group_name = '%s.%s' % (key, value)
                            self.push(self.inventory, group_name, host)
                        #将full tag string也作为hostvar和分组依据
                        self.push(self.inventory, tagstr, host)
                        #self.push(self.cache[host], 'full_tags', tagstr)

            except httplib2.HttpLib2Error, e:
                callbacks.display('[ERROR] [get_host_tags] Connect to xbox failed','red')
                sys.exit(1)
            except AnsibleXboxResponseError, e:
                callbacks.display('[ERROR] [get_host_tags] Xbox response error, http status: %s' % e,'red')
                sys.exit(1)
            except AnsibleXboxQueryError, e:
                callbacks.display('[ERROR] [get_host_tags] Query tags return empty result, %s' % e,'red')
                sys.exit(1)
            except KeyError or json.ValueError, e:
                callbacks.display('[ERROR] [get_host_tags] Decode response error, maybe xbox api changed' ,'red')
                logging.exception("")
                sys.exit(1)
            except Exception, e:
                callbacks.display('[ERROR] [get_host_tags] Unknown error: %s' % e ,'red')
                logging.exception("MARK_ANSIBLE_NEED_REVIEW")
                sys.exit(1)

    def get_host_info(self):
        """ Get variables about a specific host """

        if not self.cache or len(self.cache) == 0:
            # Need to load index from cache
            self.load_cache_from_cache()

        if not self.args.host in self.cache:
            # try updating the cache
            self.update_cache()

            if not self.args.host in self.cache:
                # host might not exist anymore
                return self.json_format_dict({}, True)

        return self.json_format_dict(self.cache[self.args.host], True)

    def is_cache_valid(self):
        """ Determines if the cache files have expired, or if it is still valid """

        if os.path.isfile(self.cache_path_inventory):
            mod_time = os.path.getmtime(self.cache_path_inventory)
            current_time = time()
            if (mod_time + self.cache_max_age) > current_time:
                if os.path.isfile(self.cache_path_cache):
                    return True

        return False

    def read_settings(self):
        """ Reads the settings from the xbox.ini file """

        config = ConfigParser.SafeConfigParser()

        # ---- 读取全局配置
        config.read(os.path.dirname(os.path.realpath(__file__)) + '/xbox_global.ini')
        # 如果客户端配置文件不存在,从这个地址下载一份拷贝
        conf_tpl_url    = config.get('xbox_global', 'conf_tpl_url')
        # 获取机器列表的url
        self.xbox_url_get_hostlist = config.get('xbox_global', 'get_host_url') 
        # 获取机器tag的url
        self.xbox_url_get_hosttags = config.get('xbox_global', 'get_tags_url')
        # 用户配置文件所在路径
        user_conf_path  = os.path.expanduser(config.get('xbox_global', 'user_conf_path'))
        # 用户cache目录所在路径
        user_cache_path = os.path.expanduser(config.get('xbox_global', 'user_cache_path'))
        # 用户可设置的cache时间间隔,如果超出范围将以此处的最大值代替
        [ cache_age_min,cache_age_max ] = config.get('xbox_global', 'valid_cache_age').split(',')
        # 禁止使用ansible的用户列表,逗号分隔
        deny_users		= config.get('xbox_global', 'deny_users').split(',')

        # ---- 封禁检查
        user = os.environ['USER']
        if user in deny_users:
            callbacks.display('[ERROR] [read_settings] User %s is denied to use ansible script, if you think it shouldn\'t, contact chengshengbo@xiaomi.com' % user)
            sys.exit(1)
        
        # ---- 初始化用户配置目录
        if not os.path.exists(user_conf_path):
            try:
                # 创建必需的目录
                conf_dir = os.path.dirname(user_conf_path)
                if not os.path.exists(conf_dir):
                    os.makedirs(conf_dir,0700)
                if not os.path.exists(user_cache_path):
                    os.makedirs(user_cache_path, 0700)
            except os.error,e:
                callbacks.display('[ERROR] [read_settings] init user conf failed ,mkdir %s or %s failed' % (user_conf_path, user_cache_path), 'red')
                sys.exit(1)

            try:
                # 从git拉取样例配置
                http = httplib2.Http()
                response, content = http.request(conf_tpl_url, "GET")
                callbacks.display('[DEBUG] [read_settings] content:[%s]' % content, log_only=True )
                file = open(user_conf_path,'w')
                file.write(content) 
                file.close()
            except IOError,e:
                callbacks.display('[ERROR] [read_settings] init user conf failed ,please ensure %s(and upper path) writeable' % user_conf_path, 'red')
                sys.exit(1)
            except httplib2.HttpLib2Error, e:
                callbacks.display('[ERROR] [read_settings] init user conf failed, download conf from %s failed' % conf_tpl_url, 'red')
                sys.exit(1)
            except Exception,e:
                callbacks.display('[ERROR] [read_settings] init user conf failed, unknown error: %s' % e, 'red' )
                sys.exit(1)
    
 
        # ---- 读取用户配置
        config.read(user_conf_path)
        # 用户的xbox token
        self.xbox_token = config.get('xbox', 'token')
        # 用户关心的tags组合,用逗号分隔
        self.xbox_groups = config.get('xbox', 'groups').split(',')
        # cache超时时间(单位:秒),用户可配置,但超出规定的范围将以全局提供的边界值代替
        self.cache_max_age = config.getint('xbox', 'cache_max_age')

        # ---- 修正用户配置
        # 未修改默认配置?
        if self.xbox_token == 'YOUR_TOKEN_STRING':
            callbacks.display('[INFO] Please edit your conf at %s first.' % user_conf_path, 'red' )
            sys.exit(0)
        # cache 失效时间不合理?
        if self.cache_max_age < int(cache_age_min):
            self.cache_max_age = cache_age_min
        elif self.cache_max_age > int(cache_age_max):
            self.cache_max_age = cache_age_max
        # cache相关路径
        self.cache_path_cache = user_cache_path + "/.ansible-xbox.cache"
        self.cache_path_inventory = user_cache_path + "/.ansible-xbox.index"

    def parse_cli_args(self):
        """ Command line argument processing """

        parser = ArgumentParser(description='Produce an Ansible Inventory file based on Xbox')
        parser.add_argument('--list', action='store_true', default=True, help='List instances (default: True)')
        parser.add_argument('--host', action='store', help='Get all the variables about a specific instance')
        parser.add_argument('--refresh-cache', action='store_true', default=False,
                            help='Force refresh of cache by making API requests to xbox (default: False - use cache files)')
        self.args = parser.parse_args()

    def update_cache(self):
        """ Make calls to xbox and save the output in a cache """

        self.get_host_list()
        self.get_host_tags()

        self.write_to_cache(self.cache, self.cache_path_cache)
        self.write_to_cache(self.inventory, self.cache_path_inventory)

    def push(self, my_dict, key, element):
        """ Pushed an element onto an array that may not have been defined in the dict """

        if key not in my_dict:
            # new tag
            my_dict[key] = [element]
        elif element not in my_dict[key]:
            # tag with new value
            my_dict[key].append(element)
        else:
            # already have this tag and value
		    pass

    def load_cache_from_cache(self):
        """ Reads the cache from the cache file sets self.cache """

        cache = open(self.cache_path_cache, 'r')
        json_cache = cache.read()
        self.cache = json.loads(json_cache)

    def load_inventory_from_cache(self):
        """ Reads the index from the cache file sets self.index """

        cache = open(self.cache_path_inventory, 'r')
        json_inventory = cache.read()
        self.inventory = json.loads(json_inventory)

    def write_to_cache(self, data, filename):
        """ Writes data in JSON format to a file """

        json_data = self.json_format_dict(data, True)
        cache = open(filename, 'w')
        cache.write(json_data)
        cache.close()

    def to_safe(self, word):
        """ Converts 'bad' characters in a string to underscores so they can be used as Ansible groups """

        return re.sub("[^A-Za-z0-9\-]", "_", word)

    def json_format_dict(self, data, pretty=False):
        """ Converts a dict to a JSON object and dumps it as a formatted string """

        if pretty:
            return json.dumps(data, sort_keys=True, indent=2)
        else:
            return json.dumps(data)


class AnsibleXboxResponseError(errors.AnsibleError):
    pass

class AnsibleXboxQueryError(errors.AnsibleError):
    pass
XboxInventory()
