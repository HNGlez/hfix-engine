#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Hugo Nistal Gonzalez
"""
import simplefix
import time


class FixSessionMessages:
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

    # TradeCaptureReport Messages (Drop Copy Gateway messages)
    def send_trade_capture_report_request(self, updates_only=False):
        msg = self.create_message(simplefix.MSGTYPE_TRADE_CAPTURE_REPORT_REQUEST)
        self._request_id = str(time.time())
        msg.append_pair(568, self._request_id)
        msg.append_pair(569, "0")
        if not updates_only:
            msg.append_pair(263, "1")
        else:
            msg.append_pair(263, "9")
        return msg

    def send_trade_capture_report_ack(self, trade_report_id):
        msg = self.create_message(simplefix.MSGTYPE_TRADE_CAPTURE_REPORT_ACK)
        msg.append_pair(571, trade_report_id)
        msg.append_pair(simplefix.TAG_SYMBOL, "NA")
        return msg

    # Market Data messages (Market Data Gateway messages)
    def market_data_request(self, symbols, request_type, book_depth, aggregate_book, unsubscribe_from=None,
                            correlation=None):
        msg = self.create_message(simplefix.MSGTYPE_MARKET_DATA_REQUEST)
        assert isinstance(symbols, list)

        md_correlation = None
        if request_type == "2":
            assert unsubscribe_from is not None
            assert correlation is not None
            msg.append_pair(262, correlation)  # MDReqID

        else:
            md_correlation = {f"{'_'.join(symbols)}_{str(request_type)}": str(int(round(time.time() * 1000)))}
            msg.append_pair(262, md_correlation)  # MDReqID
        msg.append_pair(263, request_type)  # SubscriptionRequestType
        msg.append_pair(264, book_depth)  # MarketDepth
        msg.append_pair(265, 1)  # MDUpdateType
        if request_type != "T" and unsubscribe_from != "T":
            msg.append_pair(266, aggregate_book)  # AggregatedBook

        if request_type == "1" or unsubscribe_from == "1":
            msg.append_pair(267, 2)  # NoMDEntryTypes (Repeating Group)
            msg.append_pair(269, "0")  # MDEntryType
            msg.append_pair(269, "1")
        elif request_type == "T" or unsubscribe_from == "T":
            msg.append_pair(267, 1)
            msg.append_pair(269, "2")

        msg.append_pair(146, len(symbols))  # NoRelatedSym (Repeating Group)
        for sym in symbols:
            msg.append_pair(simplefix.TAG_SYMBOL, sym)  # Symbol

        if md_correlation is not None:
            return msg, md_correlation
        else:
            return msg

    # Order Management messages (Order Management Gateway messages)
    def new_order_single(self, cl_ord_id, party_id, party_role, currency, side, symbol, quantity, price, order_type,
                         product, tif, exec_inst=None, stop_price=None, expiry_date=None, min_qty=None,
                         account_type=None, cust_order_capacity=None, precision=6):
        msg = self.create_message(simplefix.MSGTYPE_NEW_ORDER_SINGLE)
        msg.append_pair(simplefix.TAG_CLORDID, cl_ord_id)

        msg.append_pair(453, 1)  # NoPartyIDs (Repeating Group)
        msg.append_pair(448, party_id)  # PartyID
        msg.append_pair(452, party_role)  # PartyRole

        if account_type is not None:
            msg.append_pair(581, account_type)  # AccountType
        if cust_order_capacity is not None:
            msg.append_pair(582, cust_order_capacity)  # CustOrderCapacity
        msg.append_pair(simplefix.TAG_HANDLINST, simplefix.HANDLINST_AUTO_PRIVATE)
        if exec_inst is not None:
            msg.append_pair(simplefix.TAG_EXECINST, exec_inst)
        msg.append_pair(simplefix.TAG_CURRENCY, currency)
        msg.append_pair(simplefix.TAG_SIDE, side)
        msg.append_pair(simplefix.TAG_SYMBOL, symbol)
        msg.append_pair(460, product)  # Product
        msg.append_utc_timestamp(simplefix.TAG_TRANSACTTIME, precision=precision)
        msg.append_pair(simplefix.TAG_ORDERQTY, quantity)
        msg.append_pair(simplefix.TAG_ORDTYPE, order_type)
        msg.append_pair(simplefix.TAG_PRICE, price)
        if order_type == simplefix.ORDTYPE_STOP_LIMIT:
            assert stop_price is not None
            msg.append_pair(simplefix.TAG_STOPPX, stop_price)
        if tif == simplefix.TIMEINFORCE_GOOD_TILL_DATE:
            assert expiry_date is not None
            msg.append_pair(432, expiry_date)  # ExpireDate
        msg.append_pair(simplefix.TAG_TIMEINFORCE, tif)
        if min_qty is not None:
            assert tif == simplefix.TIMEINFORCE_IMMEDIATE_OR_CANCEL
            msg.append_pair(simplefix.TAG_MINQTY, min_qty)

        return msg

    def order_cancel_replace_request(self, cl_ord_id, order_id, orig_cl_ord_id, side, symbol, price, order_type,
                                     quantity=None, currency=None, product=None, tif=None, exec_inst=None,
                                     stop_price=None, expiry_date=None, min_qty=None, overfill_protection=None,
                                     account_type=None, cust_order_capacity=None, precision=6):
        msg = self.create_message(simplefix.MSGTYPE_ORDER_CANCEL_REPLACE_REQUEST)
        msg.append_pair(simplefix.TAG_ORDERID, order_id)
        msg.append_pair(simplefix.TAG_ORIGCLORDID, orig_cl_ord_id)
        msg.append_pair(simplefix.TAG_CLORDID, cl_ord_id)

        if account_type is not None:
            msg.append_pair(581, account_type)  # AccountType
        if cust_order_capacity is not None:
            msg.append_pair(582, cust_order_capacity)  # CustOrderCapacity
        msg.append_pair(simplefix.TAG_HANDLINST, simplefix.HANDLINST_AUTO_PRIVATE)
        if exec_inst is not None:
            msg.append_pair(simplefix.TAG_EXECINST, exec_inst)
        if currency is not None:
            msg.append_pair(simplefix.TAG_CURRENCY, currency)
        msg.append_pair(simplefix.TAG_SIDE, side)
        msg.append_pair(simplefix.TAG_SYMBOL, symbol)
        if product is not None:
            msg.append_pair(460, product)  # Producct
        msg.append_utc_timestamp(simplefix.TAG_TRANSACTTIME, precision=precision)
        if quantity is not None:
            msg.append_pair(simplefix.TAG_ORDERQTY, quantity)
        msg.append_pair(simplefix.TAG_ORDTYPE, order_type)
        msg.append_pair(simplefix.TAG_PRICE, price)
        if order_type == simplefix.ORDTYPE_STOP_LIMIT:
            assert stop_price is not None
            msg.append_pair(simplefix.TAG_STOPPX, stop_price)
        if tif == simplefix.TIMEINFORCE_GOOD_TILL_DATE and expiry_date is not None:
            msg.append_pair(432, expiry_date)  # ExpireDate
        if tif is not None:
            msg.append_pair(simplefix.TAG_TIMEINFORCE, tif)
        if min_qty is not None:
            assert tif == simplefix.TIMEINFORCE_IMMEDIATE_OR_CANCEL
            msg.append_pair(simplefix.TAG_MINQTY, min_qty)
        if overfill_protection is not None:
            msg.append_pair(5000, overfill_protection)  # Overfill Protection

        return msg

    def order_cancel_request(self, cancel_all=False, cl_ord_id=None, order_id=None, orig_cl_ord_id=None, side=None,
                             symbol=None, order_type=None):
        msg = self.create_message(simplefix.MSGTYPE_ORDER_CANCEL_REQUEST)
        assert isinstance(cancel_all, bool)
        if cancel_all:
            msg.append_pair(simplefix.TAG_ORDERID, "OPEN_ORDER")
            msg.append_pair(simplefix.TAG_ORIGCLORDID, "OPEN_ORDER")
            msg.append_pair(simplefix.TAG_CLORDID, "OPEN_ORDER")
            msg.append_pair(simplefix.TAG_SYMBOL, "NA")
            msg.append_pair(simplefix.TAG_SIDE, "1")
            msg.append_pair(7559, "Y")
        else:
            assert cl_ord_id is not None
            assert order_id is not None
            assert orig_cl_ord_id is not None
            assert side is not None
            assert symbol is not None
            msg.append_pair(simplefix.TAG_ORDERID, order_id)
            msg.append_pair(simplefix.TAG_ORIGCLORDID, orig_cl_ord_id)
            msg.append_pair(simplefix.TAG_CLORDID, cl_ord_id)
            msg.append_pair(simplefix.TAG_SYMBOL, symbol)
            msg.append_pair(simplefix.TAG_SIDE, side)
        msg.append_utc_timestamp(simplefix.TAG_TRANSACTTIME)
        if order_type is not None:
            msg.append_pair(simplefix.TAG_ORDTYPE, order_type)

        return msg

    def order_mass_status_request(self):
        msg = self.create_message(simplefix.MSGTYPE_ORDER_MASS_STATUS_REQUEST)
        request_id = str(int(round(time.time() * 1000)))
        msg.append_pair(584, request_id)
        msg.append_pair(585, 8)
        msg.append_utc_timestamp(simplefix.TAG_TRANSACTTIME)

        return msg, request_id
