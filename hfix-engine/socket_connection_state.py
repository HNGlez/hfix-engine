#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Hugo Nistal Gonzalez
"""
from enum import Enum


class SocketConnectionState(Enum):
    UNKNOWN = 0
    LOGGED_IN = 1
    CONNECTED = 2
    LOGGED_OUT = 3
    DISCONNECTED = 4
