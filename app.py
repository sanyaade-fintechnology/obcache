import zmq
from decimal import Decimal
import zmq.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import logging
import json
import argparse
from copy import deepcopy
from zmapi.codes import error
from zmapi.codes.error import RemoteException
from zmapi.zmq import SockRecvPublisher
import uuid
from collections import defaultdict
from sortedcontainers import SortedDict
from time import gmtime
from pprint import pprint, pformat
from datetime import datetime
from zmapi.zmq.utils import split_message, ident_to_str
from zmapi.logging import setup_root_logger
from zmapi import SubscriptionDefinition

################################## CONSTANTS ##################################

MODULE_NAME = "obcache"
DEBUG = None  # set from args

################################### GLOBALS ###################################

L = logging.root

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.cache = {}
g.startup_time = datetime.utcnow()
g.pub_bytes_in = 0
g.pub_bytes_out = 0
g.status = "ok"
g.subscriptions = defaultdict(SubscriptionDefinition)
# caps to be added to get_capabilities
g.caps_to_add = []
# caps to be removed from get_capabilities
g.caps_to_del = []

################################### HELPERS ###################################

def pack_msg(msg : dict):
    if "msg_id" not in msg:
        msg["msg_id"] = str(uuid.uuid4())
    return (" " + json.dumps(msg)).encode()

def unpack_msg(msg : bytes):
    return json.loads(msg.decode())

async def send_recv_command_raw(sock, msg):
    msg_bytes = pack_msg(msg)
    msg_id = msg["msg_id"]
    await sock.send_multipart([b"", msg_bytes])
    # this will wipe the message queue
    while True:
        msg_parts = await sock.recv_multipart()
        try:
            msg = json.loads(msg_parts[-1].decode())
            if msg["msg_id"] == msg_id:
                break
        except:
            pass
    error.check_message(msg)
    return msg["content"]

def dict_diff(x, y):
    # does not check keys that y has but x does not have
    res = {}
    for k in x:
        if k not in y:
            res[k] = x[k]
        elif x[k] != y[k]:
            res[k] = x[k]
    return res


###############################################################################

# OrderBook is programmed in a fragile way, defensive programming principles
# are not followed here. This is a sacrifice made for performance gains.
# A lot of mutation and data sharing is used and that has to be programmed
# with extreme care.

class OrderBook:

    def __init__(self, ticker_id, r2i_converter, i2r_converter, float_volume,
                 emit_quotes):
        self.r2i_conv = r2i_converter
        self.i2r_conv = i2r_converter
        # descending order
        self._bids = SortedDict(lambda x: -x)
        self._implied_bids = SortedDict(lambda x: -x)
        # ascending order
        self._asks = SortedDict()
        self._implied_asks = SortedDict()
        self._best_bid = -sys.maxsize
        self._best_ask = sys.maxsize
        self._ticker_id = ticker_id
        if float_volume:
            self._zero_size = 0.0
        else:
            self._zero_size = 0
        self.initialized = False
        self.emit_quotes = emit_quotes

    def _merge_level_to_book(self, i_price, book, new_lvl):
        """Merge level to the book.

        Returns if there are actually changes or not."""
        old_lvl = book.get(i_price)
        if not old_lvl:
            book[i_price] = old_lvl = {}
        # check whether there are any changes
        changes = False
        for k, v in new_lvl.items():
            # skip price
            if k == "price":
                continue
            if old_lvl.get(k) != v:
                changes = True
                old_lvl[k] = v
        return changes

    # mutates updates
    def _add_outside_deletions(self, levels, book, upd_book):
        old_prices = set(book.keys())
        new_prices = set([self.r2i_conv(x["price"]) for x in levels])
        deleted_prices = old_prices.difference(new_prices)
        for i_price in deleted_prices:
            L.debug("deleted {}".format(i_price))
            del book[i_price]
            upd_book[i_price] = {"size": 0}

    # mutates updates
    def _update_book(self, data, book_name, updates):
        levels = data.get(book_name)
        if not levels:
            return
        attr_name = "_" + book_name
        book = self.__dict__.get(attr_name)
        upd_book = updates[book_name]
        if not g.cap_ob_incremental:
            self._add_outside_deletions(levels, book, upd_book)
        for lvl in levels:
            i_price = self.r2i_conv(lvl["price"])
            s = "----\n{}\n{}"
            s = s.format(pformat(lvl), pformat(book.get(i_price)))
            # 0 size means the level is wiped out
            if lvl["size"] == 0:
                try:
                    del book[i_price]
                except KeyError:
                    pass
            else:
                # don't emit updates that actually change nothing
                if not self._merge_level_to_book(i_price, book, lvl):
                    continue
            # L.debug("{}: {}".format(book_name, len(book)))
            upd_lvl = book.get(i_price)
            if not upd_lvl:
                upd_lvl = {"size": 0}
            upd_book[i_price] = upd_lvl
            s += "\nupdate: {}".format(pformat(upd_lvl))

    def _repr_bbo(self):
        if self._best_bid == -sys.maxsize:
            best_bid = None
        else:
            best_bid = self.i2r_conv(self._best_bid)
        if self._best_ask == sys.maxsize:
            best_ask = None
        else:
            best_ask = self.i2r_conv(self._best_ask)
        s = "{}: bbo={}/{}".format(self._ticker_id, best_bid, best_ask)
        if best_ask and best_bid:
            s += " ({:.5%})".format(best_ask / best_bid - 1)
        return s

    def _update_bbo(self):
        changed = False
        best_bid = -sys.maxsize
        # top of the book is always highest
        if self._bids:
            best_bid = max(best_bid, self._bids.iloc[0])
        if self._implied_bids:
            best_bid = max(best_bid, self._implied_bids.iloc[0])
        bid_aggression = 0
        if self._best_bid == -sys.maxsize:
            if best_bid > -sys.maxsize:
                self._best_bid = best_bid
                # sys.maxsize is used here to express appearance of first bid.
                bid_aggression = sys.maxsize
                changed = True
        elif best_bid != self._best_bid:
            bid_aggression = best_bid - self._best_bid
            self._best_bid = best_bid
            changed = True
        best_ask = sys.maxsize
        # top of the book is always lowest
        if self._asks:
            best_ask = min(best_ask, self._asks.iloc[0])
        if self._implied_asks:
            best_ask = min(best_ask, self._implied_asks.iloc[0])
        ask_aggression = 0
        if self._best_ask is sys.maxsize:
            if best_ask < sys.maxsize:
                self._best_ask = best_ask
                # sys.maxsize is used here to express appearance of first ask.
                ask_aggression = sys.maxsize
                changed = True
        elif best_ask != self._best_ask:
            ask_aggression = self._best_ask - best_ask
            self._best_ask = best_ask
            changed = True
        if changed:
            L.debug(self._repr_bbo())
        return bid_aggression, ask_aggression

    # mutates updates
    def _sweep_book(self, book_name, is_bid, updates):
        """Remove intersecting levels from a book.

        Returns the best price of the book. If the whole book is swept,
        returns +/- `sys.maxsize`.
        """
        L.debug("sweeping book: {}".format(book_name))
        attr_name = "_" + book_name
        book = self.__dict__.get(attr_name)
        upd_book = updates[book_name]
        for i_price in book:
            # When size is 0, the other fields don't have any meaning and
            # don't need to be included.
            if is_bid and i_price >= self._best_ask:
                del book[i_price]
                upd_book[i_price] = {"size": self._zero_size}
            elif not is_bid and i_price <= self._best_bid:
                del book[i_price]
                upd_book[i_price] = {"size": self._zero_size}
            else:
                return i_price
        return -sys.maxsize if is_bid else sys.maxsize

    # mutates updates
    def _update_bbo_and_sanitize(self, updates):
        # Aggression values are used to implement sweeping logic when
        # best_bid/best_ask overlap.
        bid_aggression, ask_aggression = self._update_bbo()
        # check for overlapping levels
        if self._best_ask != sys.maxsize and self._best_bid != -sys.maxsize \
                and self._best_bid >= self._best_ask:
            L.debug("bid_aggr: {}, ask_aggr: {}"
                    .format(bid_aggression, ask_aggression))
            if ask_aggression <= 0 and bid_aggression <= 0:
                L.critical("best_bid/best_ask are overlapping and aggression "
                           "is <= 0 on both sides, assertion failed")
                sys.exit(1)
            if ask_aggression > 0 and ask_aggression < sys.maxsize \
                    and bid_aggression > 0 and bid_aggression < sys.maxsize:
                # Should not happen unless md vendor gives emits bad data.
                # In that case more dominating side will be the one overwriting
                # the other side.
                L.warning("{}: inconsistent update, bid/ask clash, "
                          "both sides aggressive".format(self._ticker_id))
            # Decided to let bid dominate in the extremely rare cases where
            # bid_aggression == ask_aggression (problematic md data feed).
            if bid_aggression >= ask_aggression:
                best_ask = sys.maxsize
                best_ask = min(best_ask,
                              self._sweep_book("asks", False, updates))
                best_ask = min(best_ask,
                              self._sweep_book("implied_asks", False, updates))
            else:
                best_bid = -sys.maxsize
                best_bid = max(best_bid,
                              self._sweep_book("bids", True, updates))
                best_bid = max(best_bid,
                              self._sweep_book("implied_bids", True, updates))
        return bid_aggression != 0, ask_aggression != 0

    def _maybe_flag_initialized(self):
        if self.initialized:
            return
        if self._bids and self._asks:
            self.initialized = True

    # mutates updates
    def _convert_update_book_to_list(self, book_name, updates, reverse):
        upd_book = updates[book_name]
        res = []
        for i_price in sorted(upd_book):
            d = {"price": self.i2r_conv(i_price)}
            d.update(upd_book[i_price])
            res.append(d)
        if reverse:
            res = res[::-1]
        updates[book_name] = res

    # mutates updates
    def _convert_updates_to_list(self, updates):
        self._convert_update_book_to_list("bids", updates, True)
        self._convert_update_book_to_list("implied_bids", updates, True)
        self._convert_update_book_to_list("asks", updates, False)
        self._convert_update_book_to_list("implied_asks", updates, False)

    # mutates updates
    def _mutate_book(self, data, updates):
        self._update_book(data, "bids", updates)
        self._update_book(data, "implied_bids", updates)
        self._update_book(data, "asks", updates)
        self._update_book(data, "implied_asks", updates)
        self._update_bbo_and_sanitize(updates)
        self._maybe_flag_initialized()
        self._convert_updates_to_list(updates)

    def _get_best_bid_lvl(self):
        i_price = self._best_bid
        if i_price == -sys.maxsize:
            return {}
        else:
            lvl = self._bids.get(i_price)
            if not lvl:
                lvl = self._implied_bids[i_price]
            d = dict(lvl)
            d["price"] = i_price
            return d

    def _get_best_ask_lvl(self):
        i_price = self._best_ask
        if i_price == sys.maxsize:
            return {}
        else:
            lvl = self._asks.get(i_price)
            if not lvl:
                lvl = self._implied_asks[i_price]
            d = dict(lvl)
            d["price"] = i_price
            return d

    def update(self, data):
        updates = {
            "bids": {},
            "asks": {},
            "implied_bids": {},
            "implied_asks": {},
        }
        if self.emit_quotes:
            prev_bb = self._get_best_bid_lvl()
            prev_ba = self._get_best_ask_lvl()
        self._mutate_book(data, updates)
        quotes = {}
        if self.emit_quotes:
            # Should be ok to compare floats because no arithmetic operations
            # have been performed with them?
            diff = dict_diff(self._get_best_bid_lvl(), prev_bb)
            if diff:
                if "price" in diff:
                    diff["price"] = self.i2r_conv(diff["price"])
                for k, v in diff.items():
                    quotes["bid_" + k] = v
            diff = dict_diff(self._get_best_ask_lvl(), prev_ba)
            if diff:
                if "price" in diff:
                    diff["price"] = self.i2r_conv(diff["price"])
                for k, v in diff.items():
                    quotes["ask_" + k] = v
            if quotes and "timestamp" in data:
                quotes["timestamp"] = data["timestamp"]
        # for k, v in updates.items():
        #     if v:
        #         print("updates:", k, len(v))
        return updates, quotes

class TruncatedOrderBook(OrderBook):
    """OrderBook with order_book_levels emulation."""

    def __init__(self, ticker_id, r2i_converter, i2r_converter, float_volume,
                 emit_quotes, max_levels):
        super().__init__(ticker_id, r2i_converter, i2r_converter, float_volume,
                         emit_quotes)
        # descending order
        self._bids_tr = deepcopy(self._bids)
        self._implied_bids_tr = deepcopy(self._implied_bids)
        # ascending order
        self._asks_tr = deepcopy(self._asks)
        self._implied_asks_tr = deepcopy(self._implied_asks)
        self.max_levels = max_levels

    def _update_truncated_books(self, updates):
        for book_name, upd_book in updates.items():
            book = self.__dict__.get("_" + book_name)
            book_tr = self.__dict__.get("_" + book_name + "_tr")
            # update book_tr based on updates
            for i_price, upd_lvl in upd_book.items():
                if upd_lvl["size"] == 0:
                    try:
                        del book_tr[i_price]
                    except KeyError:
                        pass
                else:
                    book_tr[i_price] = upd_lvl
            tr_len = len(book_tr)
            if tr_len < self.max_levels:
                # Check for potential redisplay of data. This is why full books
                # are required.
                i_prices_to_add = book.iloc[tr_len:self.max_levels]
                for i_price in i_prices_to_add:
                    lvl = book[i_price]
                    book_tr[i_price] = lvl
                    upd_book[i_price] = lvl
            elif tr_len > self.max_levels:
                i_prices_to_del = book_tr.iloc[self.max_levels:]
                for i_price in i_prices_to_del:
                    del book_tr[i_price]
                    upd_book[i_price] = {"size": 0}
            if DEBUG:
                assert len(book_tr) <= self.max_levels, len(book_tr)
                if len(book) >= self.max_levels:
                    assert len(book_tr) == self.max_levels, len(book_tr)
                for i_price, lvl in book_tr.items():
                    assert lvl == book[i_price]

    def _mutate_book(self, data, updates):
        self._update_book(data, "bids", updates)
        self._update_book(data, "implied_bids", updates)
        self._update_book(data, "asks", updates)
        self._update_book(data, "implied_asks", updates)
        self._update_bbo_and_sanitize(updates)
        self._update_truncated_books(updates)
        self._maybe_flag_initialized()
        self._convert_updates_to_list(updates)

################################ MD CONVERTER #################################

async def get_ticker_info(ticker_id: str):
    msg_id = str(uuid.uuid4())
    data = {
        "command": "get_ticker_info",
        "msg_id": msg_id,
        "content": {
            "ticker": {"ticker_id": ticker_id}
        }
    }
    msg = " " + json.dumps(data)
    msg = msg.encode()
    await g.sock_deal.send_multipart([b"", msg])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg_id)
    msg = json.loads(msg_parts[1].decode())
    error.check_message(msg)
    content = msg["content"]
    assert len(content) == 1, len(content)
    return content[0]

def get_converters(data):
    """Get converters that convert between raw price and integer price."""
    if data["float_price"]:
        ts = data["price_tick_size"]
        if isinstance(ts, list):
            # use the smallest tick size
            ts = tick_size[0][1]
        # is this a good way to get number of decimals?
        num_decimals = -Decimal(str(ts)).as_tuple().exponent
        ts = 1.0 * 10 ** -num_decimals
        L.debug("{}: min_tick={}".format(data["ticker_id"], ts))
        def r2i_converter(x):
            return round(x / ts)
        def i2r_converter(x):
            return round(x * ts, num_decimals)
    else:
        # If prices are in int format no conversion needed =>
        # return identity functions.
        def r2i_converter(x):
            return x
        def i2r_converter(x):
            return x
    return r2i_converter, i2r_converter

async def send_quotes(ticker_id : bytes, quotes):
    msg_bytes = (" " + json.dumps(quotes)).encode()
    g.sock_pub.send_multipart([ticker_id + b"\x03", msg_bytes])

# mutates data
async def convert_data(ticker_id: bytes, data):
    cache = g.cache.get(ticker_id)
    ticker_id_str = ticker_id.decode()
    sub_def = g.subscriptions[ticker_id_str]
    if not cache:
        g.cache[ticker_id] = cache = {}
        # Don't need to send clear updates if order_book_levels is 0 at
        # application startup time.
        L.info("fetching ticker info for {} ...".format(ticker_id_str))
        try:
            ticker_info = await get_ticker_info(ticker_id_str)
            r2i_converter, i2r_converter = get_converters(ticker_info)
        except Exception as err:
            L.exception("failed to fetch ticker_info for {}:"
                        .format(ticker_id_str))
            L.warning("ignored {}".format(ticker_id_str))
            g.cache[ticker_id] = 0
            return data
        L.info("price converter constructed for: {}".format(ticker_id_str))
        emit_quotes = sub_def.emit_quotes
        if g.cap_ob_levels:
            L.info("creating new order book for {} ...".format(ticker_id_str))
            cache["order_book"] = OrderBook(ticker_id,
                                            r2i_converter,
                                            i2r_converter,
                                            ticker_info["float_volume"],
                                            emit_quotes)
        else:
            max_levels = sub_def.order_book_levels
            L.info("creating new order book for {} (max_levels: {}) ..."
                   .format(ticker_id_str, max_levels))
            cache["order_book"] = TruncatedOrderBook(
                    ticker_id, r2i_converter, i2r_converter, 
                    ticker_info["float_volume"], emit_quotes, max_levels)
        cache["cleared"] = True
    ob = cache["order_book"]
    updates, quotes = ob.update(data)
    data.pop("bids", None)
    data.pop("asks", None)
    data.pop("implied_bids", None)
    data.pop("implied_asks", None)
    if quotes:
        await send_quotes(ticker_id, quotes)
    if sub_def.order_book_levels > 0 or not cache["cleared"]:
        if updates["bids"]:
            data["bids"] = updates["bids"]
        if updates["implied_bids"]:
            data["implied_bids"] = updates["implied_bids"]
        if updates["asks"]:
            data["asks"] = updates["asks"]
        if updates["implied_asks"]:
            data["implied_asks"] = updates["implied_asks"]
        cache["cleared"] = True
        return data
    else:
        # don't emit any depth updates
        return None

async def convert_md_msg(ticker_id: bytes, msg):
    if g.cache.get(ticker_id) == 0:  # ignored ticker_id
        return msg
    if not msg:
        # unsubscribed, delete cached data
        try:
            del g.cache[ticker_id]
        except KeyError:
            pass
        return msg
    data = json.loads(msg.decode())
    data = await convert_data(ticker_id, data)
    if not data:
        return None
    return (" " + json.dumps(data)).encode()

async def handle_md_msg(ticker_id: bytes, msg_parts):
    g.pub_bytes_in += sum([len(x) for x in msg_parts])
    topic = msg_parts[0]
    try:
        msg_bytes = await convert_md_msg(ticker_id, msg_parts[1])
    except Exception as err:
        L.exception("exception when converting message:", err)
    else:
        if msg_bytes:
            g.pub_bytes_out += len(topic) + len(msg_bytes)
            await g.sock_pub.send_multipart([topic, msg_bytes])

async def run_md_converter():
    L.info("running md converter coroutine...")
    while True:
        msg_parts = await g.sock_sub.recv_multipart()
        ticker_id = msg_parts[0][:-1]
        if ticker_id.decode() not in g.subscriptions:
            # skip message if subscription doesn't exist
            continue
        msg_type = msg_parts[0][-1]
        if msg_type != 1:
            await g.sock_pub.send_multipart(msg_parts)
            continue
        create_task(handle_md_msg(ticker_id, msg_parts))

############################### CTL INTERCEPTOR ###############################

async def send_error(ident, msg_id, ecode, msg=None):
    msg = error.gen_error(ecode, msg)
    msg["msg_id"] = msg_id
    msg = " " + json.dumps(msg)
    msg = msg.encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg])

async def fwd_message_no_change(msg_id, msg_parts):
    await g.sock_deal.send_multipart(msg_parts)
    res = await g.sock_deal_pub.poll_for_msg_id(msg_id)
    await g.sock_ctl.send_multipart(res)
    return res

def construct_ob_levels_from_cache(cache, num_levels):
    ob = cache["order_book"]
    def handle_book(book):
        res = []
        for i, kvp in enumerate(book.items()):
            if i == num_levels:
                break
            i_price, lvl = kvp
            price = ob.i2r_conv(i_price)
            d = {"price": price}
            d.update(lvl)
            res.append(d)
        return res
    bids = handle_book(ob._bids)
    implied_bids = handle_book(ob._implied_bids)
    asks = handle_book(ob._asks)
    implied_asks = handle_book(ob._implied_asks)
    res = {
        "asks": asks,
        "bids": bids,
        "implied_asks": implied_asks,
        "implied_bids": implied_bids,
    }
    return res

async def handle_get_snapshot(ident, msg):
    content = msg["content"]
    ticker_id = content["ticker_id"]
    ob = None
    ob_levels = content.get("order_book_levels", 0)
    if ob_levels > 0 and ticker_id:
        cache = g.cache.get(ticker_id.encode())
        if cache and "order_book" in cache and cache["order_book"].initialized:
            ob = construct_ob_levels_from_cache(cache, ob_levels)
    if ob:
        L.info("contructed order book from cache for ticker_id: {}"
               .format(ticker_id))
        content["order_book_levels"] = 0
    else:
        L.info("retrieving order book from upstream ctl for ticker_id: {}..."
               .format(ticker_id))
    msg_bytes = " " + json.dumps(msg)
    msg_bytes = msg_bytes.encode()
    await g.sock_deal.send_multipart(ident + [b"", msg_bytes])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    res = json.loads(msg_parts[-1].decode())
    if res["result"] != "ok":
        await g.sock_ctl.send_multipart(msg_parts)
        return
    if ob_levels > 0 and ob is not None:
        res["content"]["order_book"] = ob
    msg_bytes = " " + json.dumps(res)
    msg_bytes = msg_bytes.encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg_bytes])

async def handle_get_status(ident, msg):
    msg_bytes = (" " + json.dumps(msg)).encode()
    await g.sock_deal.send_multipart(ident + [b"", msg_bytes])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    msg = json.loads(msg_parts[-1].decode())
    content = msg["content"]
    status = {
        "name": MODULE_NAME,
        "num_cached_order_books": len([x for x in g.cache if x != 0]),
        "status": g.status,
        "uptime": (datetime.utcnow() - g.startup_time).total_seconds(),
        "pub_bytes_in": g.pub_bytes_in,
        "pub_bytes_out": g.pub_bytes_out,
    }
    content = [status] + content
    msg["content"] = content
    msg_bytes = (" " + json.dumps(msg)).encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg_bytes])

async def handle_modify_subscription(ident, msg, msg_raw):
    content = msg["content"]
    content_mod = dict(content)
    ticker_id = content_mod.pop("ticker_id")
    old_sub_def = g.subscriptions[ticker_id]
    new_sub_def = deepcopy(old_sub_def)
    new_sub_def.update(content_mod)
    # if not g.cap_pub_quotes and new_sub_def.emit_quotes:
    #     # Need order book subscription to emulate emit_quotes.
    #     new_sub_def.order_book_speed = \
    #             max(1, new_sub_def.order_book_speed)
    #     new_sub_def.order_book_levels = \
    #             max(1, new_sub_def.order_book_levels)
    if new_sub_def.empty():
        g.subscriptions.pop(ticker_id, "")
        g.cache.pop(ticker_id.encode(), None)
    else:
        g.subscriptions[ticker_id] = new_sub_def
    await g.sock_deal.send_multipart(ident + [b"", msg_raw])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    msg_bytes = msg_parts[-1]
    msg = json.loads(msg_bytes.decode())
    if msg["result"] == "ok":
        cache = g.cache.get(ticker_id.encode())
        if cache:
            ob = cache["order_book"]
            s = ""
            if not g.cap_ob_levels:
                if new_sub_def.order_book_levels != \
                        old_sub_def.order_book_levels:
                    # If order_book_levels is turned to 0, clearing updates
                    # shall be emitted.
                    cache["cleared"] = False
                    ob.max_levels = new_sub_def.order_book_levels
                    s += " max_levels={}".format(ob.max_levels)
            if not g.cap_pub_quotes:
                if not new_sub_def.emit_quotes != \
                        old_sub_def.emit_quotes:
                    ob.emit_quotes = new_sub_def.emit_quotes
                    s += " emit_quotes={}".format(ob.emit_quotes)
            if s:
                L.info("{}:{}".format(ticker_id, s))
    else:
        # revert changes
        g.subscriptions[ticker_id] = old_sub_def
    await g.sock_ctl.send_multipart(ident + [b"", msg_bytes])

async def handle_ctl_msg_1(ident, msg_raw):
    msg = json.loads(msg_raw.decode())
    msg_id = msg["msg_id"]
    cmd = msg["command"]
    debug_str = "ident={}, command={}, msg_id={}"
    debug_str = debug_str.format(ident_to_str(ident), cmd, msg["msg_id"])
    L.debug("> " + debug_str)
    try:
        if cmd == "get_snapshot":
            await handle_get_snapshot(ident, msg)
        elif cmd == "get_status":
            await handle_get_status(ident, msg)
        elif cmd == "modify_subscription":
            await handle_modify_subscription(ident, msg, msg_raw)
        else:
            await fwd_message_no_change(msg_id, ident + [b"", msg_raw])
    except Exception as e:
        L.exception("exception on msg_id: {}".format(msg_id))
        await send_error(ident, msg_id, error.GENERIC, str(e))
    L.debug("< " + debug_str)

async def run_ctl_interceptor():
    L.info("running ctl interceptor coroutine ...")
    while True:
        msg_parts = await g.sock_ctl.recv_multipart()
        try:
            ident, msg = split_message(msg_parts)
        except ValueError as err:
            L.error(str(err))
            continue
        if len(msg) == 0:
            # handle ping message
            await g.sock_deal.send_multipart(msg_parts)
            res = await g.sock_deal_pub.poll_for_pong()
            await g.sock_ctl.send_multipart(res)
            continue
        create_task(handle_ctl_msg_1(ident, msg))

###############################################################################

def parse_args():
    desc = "order book caching middleware"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_up",
                        help="address of the upstream ctl socket")
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("pub_addr_up",
                        help="address of the upstream pub socket")
    parser.add_argument("pub_addr_down",
                        help="pub socket binding address")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    parser.add_argument("--debug", action="store_true",
                        help="enable debug mode")
    parser.add_argument("-a", "--all-levels", action="store_true",
                        help="do not emulate order_book_levels")
    parser.add_argument("--no-quotes", action="store_true",
                        help="do not emulate quotes updates")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args

def setup_logging(args):
    setup_root_logger(args.log_level)

def init_zmq_sockets(args):
    g.sock_deal = g.ctx.socket(zmq.DEALER)
    g.sock_deal.setsockopt_string(zmq.IDENTITY, MODULE_NAME)
    g.sock_deal.connect(args.ctl_addr_up)
    g.sock_deal_pub = SockRecvPublisher(g.ctx, g.sock_deal)
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr_down)
    g.sock_sub = g.ctx.socket(zmq.SUB)
    g.sock_sub.connect(args.pub_addr_up)
    g.sock_sub.subscribe(b"")
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr_down)

async def init_check_capabilities(args):
    msg = {"command": "list_capabilities"}
    caps = await send_recv_command_raw(g.sock_deal, msg)
    if "GET_TICKER_INFO_PRICE_TICK_SIZE" not in caps:
        L.critical("MD does not support GET_TICKER_INFO_PRICE_TICK_SIZE cap")
        sys.exit(1)
    g.cap_ob_incremental = "PUB_ORDER_BOOK_INCREMENTAL" in caps
    g.cap_sane_ob = "PUB_SANE_ORDER_BOOK" in caps
    g.cap_ob_levels = "PUB_ORDER_BOOK_LEVELS" in caps
    g.cap_pub_quotes = "PUB_BBA_QUOTES" in caps
    if g.cap_sane_ob:
        if not g.cap_ob_incremental:
            L.critical("illegal caps: PUB_SANE_ORDER_BOOK defined "
                       "without PUB_ORDER_BOOK_INCREMENTAL")
            sys.exit(1)
        if not g.cap_ob_levels:
            L.critical("illegal caps: PUB_ORDER_BOOK_LEVELS defined "
                       "without PUB_SANE_ORDER_BOOK")
            sys.exit(1)
    g.cap_ob_levels |= args.all_levels
    g.cap_pub_quotes |= args.no_quotes
    if g.cap_sane_ob and g.cap_ob_levels and g.cap_pub_quotes:
        L.critical("nothing to do, please remove this module from this chain")
        sys.exit(1)

async def init_get_subscriptions():
    msg = {"command": "get_subscriptions"}
    subs = await send_recv_command_raw(g.sock_deal, msg)
    subs = {k: SubscriptionDefinition(**v) for k, v in subs.items()}
    g.subscriptions.update(subs)
    # if not g.cap_pub_quotes:
    #     for tid, d in g.subscriptions.items():
    #         if d.emit_quotes:
    #             # Need order book subscription to emulate emit_quotes.
    #             before = deepcopy(d)
    #             d.order_book_speed = max(1, d.order_book_speed)
    #             d.order_book_levels = max(1, d.order_book_levels)
    #             if before != d:
    #                 msg = {"command": "modify_subscription"}
    #                 content = {"ticker_id": tid}
    #                 content.update(d.__dict__)
    #                 msg["content"] = content
    #                 L.info("modifying existing subscription to {} ..."
    #                        .format(tid))
    #                 await send_recv_command_raw(g.sock_deal, msg)

def main():
    global DEBUG
    args = parse_args()
    DEBUG = args.debug
    setup_logging(args)
    if DEBUG:
        L.info("debug mode activated")
    init_zmq_sockets(args)
    L.info("checking md capabilities ...")
    g.loop.run_until_complete(init_check_capabilities(args))
    L.info("md capabilities ok")
    if not g.cap_ob_levels:
        g.loop.run_until_complete(init_get_subscriptions())
        if g.subscriptions:
            L.info("{} active subscription(s) found"
                   .format(len(g.subscriptions)))
    tasks = [
        run_md_converter(),
        run_ctl_interceptor(),
        g.sock_deal_pub.run(),
    ]
    tasks = [create_task(coro_obj) for coro_obj in tasks]
    g.loop.run_until_complete(asyncio.gather(*tasks))


if __name__ == "__main__":
    main()
