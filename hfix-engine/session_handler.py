#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import simplefix


class FIXSessionHandler:
    """Class to handle FIX session level validations."""
    def __init__(self, target_comp_id, sender_comp_id):
        self._target_comp_id = target_comp_id
        self._sender_comp_id = sender_comp_id
        self._outbound_seq_no = 0
        self._next_expected_seq_no = 1

    def validate_comp_ids(self, target_comp_id: str, sender_comp_id: str) -> bool:
        """Validate if the given TargetCompId and SenderCompId match with the configuration values.

        Parameters
        ----------
        target_comp_id: str
            Target Comp ID.
        sender_comp_id: str
            Sender Comp ID.

        Returns
        -------
        bool
            True if Sender Comp ID and Target Comp ID match the configured values else False.
        """
        return self._target_comp_id == target_comp_id and self._sender_comp_id == sender_comp_id

    def validate_recv_seq_no(self, msg_seq_no: str or int):
        """Validate that the received sequence number matches the expected sequence number.

        Parameters
        ----------
        msg_seq_no: str or int
            Sequence number from an inbound FIX message

        Returns
        -------
        bool
            True if the given sequence number if equal to the expected sequence number else False
        int
            Next expected sequence number if validation is False else the given message sequence number
        """
        if self._next_expected_seq_no < int(msg_seq_no):
            return False, self._next_expected_seq_no
        else:
            return True, msg_seq_no

    def reset_seq_no(self):
        """Resets sequence numbers. Outbound sequence number reset to 0 and next expected inbound sequence
        number reset to 1.
        """
        self._outbound_seq_no = 0
        self._next_expected_seq_no = 1

    def update_recv_seq_no(self, msg_seq_no: str or int):
        """Increase the next expected sequence number by 1.

        Parameters
        ----------
        msg_seq_no: str or int
            Message sequence number.
        """
        self._next_expected_seq_no = int(msg_seq_no) + 1

    def sequence_num_handler(self, message: simplefix.FixMessage):
        """Append the correct sequence number to FIX message.

        Parameters
        ----------
        message: simplefix.FixMessage
            FIX Message.
        """
        assert isinstance(message, simplefix.FixMessage)
        self._outbound_seq_no += 1
        message.append_pair(simplefix.TAG_MSGSEQNUM, self._outbound_seq_no, header=True)
