#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Hugo Nistal Gonzalez
"""

import asyncio
import simplefix
import logging
import time
from session_handler import FIXSessionHandler
from aiolimiter import AsyncLimiter
from socket_connection_state import SocketConnectionState
from fix_client_messages import FixBusinessMessages
import configparser
from uuid import uuid4


class FIXConnectionHandler(object):
    def __init__(self, config_file, gateway, listener, loop):
        self._config = self._load_config(config_file, gateway)
        self._connection_state = SocketConnectionState.DISCONNECTED
        self._reader = None
        self._writer = None
        self._fix_parser = simplefix.FixParser()
        self._session = FIXSessionHandler(self._config["TargetCompID"], self._config["SenderCompID"])
        self._client_message = FixBusinessMessages(self._config['SenderCompID'], self._config['TargetCompID'],
                                                   self._config['SenderPassword'], self._config['BeginString'],
                                                   self._config.getint('HeartBeatInterval'))
        self._listener = listener
        self._last_rcv_msg = time.time()
        self._last_sent_msg = time.time()
        self._missed_heartbeats = 0
        self._last_logon_attempt = 0
        self._logon_count = 0
        self._buffer_size = 150
        self._rate_limiter = AsyncLimiter(self._config.getint("MaxMessagesNo"),
                                          self._config.getint("MaxMessagesPeriodInSec"))
        self._heartbeat_interval = self._config.getint("HeartBeatInterval")
        self._fix_logger = self._setup_logger(name=self._config["BeginString"],
                                              filename=f"{self._config['SenderCompID']}-fixMessages")
        self._engine_logger = self._setup_logger(name=self._config["SenderCompID"],
                                                 filename=f"{self._config['SenderCompID']}-session",
                                                 formatter="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        asyncio.ensure_future(self._engine_read_loop(), loop=loop)
        asyncio.ensure_future(self._heartbeat_loop(), loop=loop)

    @property
    def connection_state(self):
        return self._connection_state

    async def _engine_read_loop(self):
        self._reader, self._writer = await asyncio.open_connection(self._config["SocketHost"],
                                                                   self._config["SocketPort"])
        self._connection_state = SocketConnectionState.CONNECTED
        self._engine_logger.info(f"Socket Connection Open to {self._config['SocketHost']}:{self._config['SocketPort']}")

        await self._logon()

        while self._connectionState != SocketConnectionState.DISCONNECTED:
            if self._connectionState != SocketConnectionState.LOGGED_OUT:
                await self._read_message()
            else:
                await self._logon()

    async def _heartbeat_loop(self):
        while self._connectionState != SocketConnectionState.DISCONNECTED:
            if self._connectionState != SocketConnectionState.LOGGED_OUT:
                await self._send_heartbeat()
                await self._is_expected_heartbeat()

    @staticmethod
    def _load_config(file_path, gateway):
        parser = configparser.SafeConfigParser()
        parser.read(file_path)
        if parser.has_section(gateway):
            return parser[gateway]
        else:
            raise Exception(f"{gateway} section not found in configuration file {file_path}")

    def _setup_logger(self, name, filename, level=logging.INFO, formatter="%(asctime)s - %(message)s"):
        handler = logging.FileHandler(filename=f"{self._config['FileLogPath']}/{filename}.log")
        handler.setFormatter(logging.Formatter(formatter))
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        return logger

    async def disconnect(self):
        """Disconnect Session."""
        if self._connection_state != SocketConnectionState.LOGGED_OUT:
            await self._logout()
        await self._handle_close()

    async def _handle_close(self):
        """Handle Close Writer Socket Connection."""
        if self._connection_state == SocketConnectionState.DISCONNECTED:
            self._engine_logger.info(f"{self._config['SenderCompID']} session -> DISCONNECTED")
            self._writer.close()
            self._connection_state = SocketConnectionState.DISCONNECTED

    async def send_message(self, message: simplefix.FixMessage):
        """Send FIX Message to Server.

        Parameters
        ----------
        message: simplefix.FixMessage
            FIX Message.
        """
        if (self._connection_state != SocketConnectionState.CONNECTED and
                self._connection_state != SocketConnectionState.LOGGED_IN):
            self._engine_logger.warning("Cannot Send Message. Socket is closed or Session is LOGGED OUT")
            return
        self._session.sequence_num_handler(message)
        message = message.encode()
        async with self._rate_limiter:
            self._writer.write(message)
            await self._writer.drain()
            self._last_sent_msg = time.time()
        self._fix_logger.info(f"{FIXConnectionHandler.print_fix(message)}")

    async def _read_message(self):
        """Read message from the TCP socket connection and parse it into a simplefix Message structure.

        Raises
        ------
        ConnectionError
            If the TCP socket is closed but the application is trying to read a message from it.
        CancelledError
            If a timeout occurs while reading a message from the TCP socket.
        """
        try:
            message = None
            while message is None:
                buffer = await self._reader.read(self._buffer_size)
                if not buffer:
                    break

                self._fix_parser.append_buffer(buffer)
                message = self._fix_parser.get_message()

            if message is None:
                return
            self._fix_logger.info(f"{message}")
            await self._process_message(message)
        except ConnectionError as e:
            self._engine_logger.error("Connection Closed Unexpected.", exc_info=True)
            raise e
        except asyncio.CancelledError as e:
            self._engine_logger.error(f"{self._config['SenderCompID']} Read Message Timed Out", exc_info=True)
            raise e
        except Exception as e:
            self._engine_logger.error("Error reading message", exc_info=True)
            raise e

    async def _process_message(self, message: simplefix.FixMessage):
        """Process incoming FIX Message and update session state.

        Parameters
        ----------
        message: simplefix.FixMessage
            FIX Message.
        """
        self._last_rcv_msg = time.time()
        self._missed_heartbeats = 0
        begin_string = message.get(simplefix.TAG_BEGINSTRING).decode()

        if begin_string != self._config['BeginString']:
            self._engine_logger.warning(
                f"FIX Protocol is incorrect. Expected: {self._config['BeginString']}; Received: {begin_string}")
            await self.disconnect()
            return

        if not await self._session_message_handler(message):
            await self._message_notification(message)
        recv_seq_no = message.get(simplefix.TAG_MSGSEQNUM).decode()

        seq_no_state, expected_seq_no = self._session.validate_recv_seq_no(recv_seq_no)
        if not seq_no_state:
            self._engine_logger.warning(f"Received Sequence Number not expected. "
                                        f"Received: {recv_seq_no}; Expected {expected_seq_no}")
            # Unexpected sequence number. Send resend request
            self._engine_logger.info(f"Sending Resend Request of messages: {expected_seq_no} to {recv_seq_no}")
            msg = self._client_message.send_resend_request(expected_seq_no, recv_seq_no)
            await self.send_message(msg)
        else:
            self._session.update_recv_seq_no(recv_seq_no)

    async def _message_notification(self, message):
        """Send FIX Message to registered listener.

        Parameters
        ----------
        message: simplefix.FixMessage
            FIX Message.
        """
        await self._listener(message)

    async def _logon(self):
        """Handle Logon."""
        if self._logon_count >= self._config.getint('MaxReconnectAttempts'):
            self._engine_logger.error(f"Max Logon attempts ({self._config.getint('MaxReconnectAttempts')}) reached."
                                      f" Disconnecting")
            await self.disconnect()
        self._engine_logger.info(f"{self._config['SenderCompID']} session -> Sending LOGON")
        if time.time() - self._last_logon_attempt > self._config.getint('ReconnectInterval'):
            msg = self._client_message.send_log_on()
            await self.send_message(msg)
            self._last_logon_attempt = time.time()

    async def _logout(self):
        """Handle Logout."""
        self._engine_logger.info(f"{self._config['SenderCompID']} session -> Sending LOGOUT")
        self._connection_state = SocketConnectionState.LOGGED_OUT
        await self.send_message(self._client_message.send_log_out())

    async def _is_expected_heartbeat(self):
        """Check if a heartbeat should have been received. If it should have been received, but it hasn't,
        then send a Test Request message to verify if the connection is healthy.
        If the configured MaxMissedHeartBeats is reached, then close the connection."""
        if time.time() - self._last_rcv_msg > self._heartbeat_interval:
            self._engine_logger.warning(f"Heartbeat expected not received. "
                                        f"Missed Heartbeats: {self._missed_heartbeats}")
            self._missed_heartbeats += 1
            await self.send_message(self._client_message.send_test_request(uuid4()))
        if self._missed_heartbeats >= self._config.getint('MaxMissedHeartBeats'):
            self._engine_logger.error(f"Max Missed Heartbeats ({self._config.getint('MaxMissedHeartBeats')}) reached."
                                      f" Loging out")
            await self._logout()

    async def _send_heartbeat(self):
        if time.time() - self._last_sent_msg > self._heartbeat_interval:
            await self.send_message(self._client_message.send_heartbeat())

    async def _session_message_handler(self, message: simplefix.FixMessage) -> bool:
        """ Handle Session Message.

        Parameters
        ---------
        message: simplefix.FixMessage
            FIX Message.

        Returns
        -------
        bool
            True if the message was a session business message else False
        """
        # NEED TO ADD HANDLING OF BUSINESS REJECTS

        msg_type = message.get(simplefix.TAG_MSGTYPE)
        if msg_type == simplefix.MSGTYPE_LOGON:  # Handle logon
            if self._connectionState == SocketConnectionState.LOGGED_IN:
                if message.get(simplefix.TAG_RESETSEQNUMFLAG) == simplefix.RESETSEQNUMFLAG_YES:
                    self._session.reset_seq_no()
                    self._engine_logger.info("Resetting Sequence Number to 1")
                    self._engine_logger.info(f"{self._config['SenderCompID']} already logged in "
                                             f"-> Ignoring Logon Response.")
                return True

            else:
                self._connectionState = SocketConnectionState.LOGGED_IN
                self._engine_logger.info(f"{self._config['SenderCompID']} session -> LOGON")
                self._heartbeat_interval = str(message.get(simplefix.TAG_HEARTBTINT).decode())
                return True
        elif self._connectionState == SocketConnectionState.LOGGED_IN:
            if msg_type == simplefix.MSGTYPE_TEST_REQUEST:
                msg = self._client_message.send_heartbeat()
                msg.append_pair(simplefix.TAG_TESTREQID, message.get(simplefix.TAG_TESTREQID))
                await self.send_message(msg)
                return True
            elif msg_type == simplefix.MSGTYPE_LOGOUT:  # Handle Logout
                self._connectionState = SocketConnectionState.LOGGED_OUT
                self._engine_logger.info(f"{self._config['SenderCompID']} session -> LOGOUT")
                await self._handle_close()
                return True
            elif msg_type == simplefix.MSGTYPE_HEARTBEAT:
                return True
            else:
                return False
        else:
            self._engine_logger.warning(f"Cannot process message. {self._config['SenderCompID']} is not logged in.")
            return False

    @staticmethod
    def print_fix(msg):
        """Print FIX message to string."""
        return msg.replace(b"\x01", b"|").decode()
