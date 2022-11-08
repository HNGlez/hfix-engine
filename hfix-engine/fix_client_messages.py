#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Hugo Nistal Gonzalez
"""

import simplefix


class FixBusinessMessages:
    def __init__(self, sender_comp_id, target_comp_id, password, fix_version, heartbeat_interval):
        self._sender_comp_id = sender_comp_id
        self._target_comp_id = target_comp_id
        self._password = password
        self._fix_version = fix_version
        self._heartbeat_interval = heartbeat_interval
        self._request_id = None

    def create_message(self, message_type: bytes) -> simplefix.FixMessage():
        """ Creates Basic Structure of FIX Message. """

        msg = simplefix.FixMessage()
        msg.append_pair(simplefix.TAG_BEGINSTRING, self._fix_version)
        msg.append_pair(simplefix.TAG_MSGTYPE, message_type)
        msg.append_pair(simplefix.TAG_SENDER_COMPID, self._sender_comp_id)
        msg.append_pair(simplefix.TAG_TARGET_COMPID, self._target_comp_id)
        msg.append_utc_timestamp(simplefix.TAG_SENDING_TIME, header=True)
        return msg

    # Business Messages
    def send_log_on(self, reset_seq_no=simplefix.RESETSEQNUMFLAG_YES):
        msg = self.create_message(simplefix.MSGTYPE_LOGON)
        msg.append_pair(simplefix.TAG_ENCRYPTMETHOD, simplefix.ENCRYPTMETHOD_NONE)
        msg.append_pair(simplefix.TAG_HEARTBTINT, self._heartbeat_interval)
        msg.append_pair(simplefix.TAG_RESETSEQNUMFLAG, reset_seq_no)
        msg.append_pair(554, self._password)
        return msg

    def send_log_out(self):
        msg = self.create_message(simplefix.MSGTYPE_LOGOUT)
        return msg

    def send_resend_request(self, begin_seq_no, end_seq_no):
        msg = self.create_message(simplefix.MSGTYPE_RESEND_REQUEST)
        msg.append_pair(simplefix.TAG_BEGINSEQNO, begin_seq_no)
        msg.append_pair(simplefix.TAG_ENDSEQNO, end_seq_no)
        return msg

    def send_heartbeat(self):
        return self.create_message(simplefix.MSGTYPE_HEARTBEAT)

    def send_test_request(self, request_id):
        msg = self.create_message(simplefix.MSGTYPE_TEST_REQUEST)
        msg.append_pair(simplefix.TAG_TESTREQID, request_id)
        return msg

    def send_change_password_request(self, new_password):
        msg = self.create_message(simplefix.MSGTYPE_USER_REQUEST)
        msg.append_pair(924, "3")
        msg.append_pair(553, self._sender_comp_id)
        msg.append_pair(554, self._password)
        msg.append_pair(925, new_password)
        return msg
