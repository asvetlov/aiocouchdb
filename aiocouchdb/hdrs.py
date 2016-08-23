# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from aiohttp.hdrs import *
from multidict import upstr

#: Defines CouchDB Proxy Auth username
X_AUTH_COUCHDB_USERNAME = upstr('X-Auth-CouchDB-UserName')
#: Defines CouchDB Proxy Auth list of roles separated by a comma
X_AUTH_COUCHDB_ROLES = upstr('X-Auth-CouchDB-Roles')
#: Defines CouchDB Proxy Auth token
X_AUTH_COUCHDB_TOKEN = upstr('X-Auth-CouchDB-Token')
