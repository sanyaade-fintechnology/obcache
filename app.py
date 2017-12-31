import zmq
import zmq.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import logging
import json
import argparse
from zmapi.codes import error
from zmapi.zmq import SockRecvPublisher
import uuid
from sortedcontainers import SortedDict
from time import gmtime
from pprint import pprint, pformat
from datetime import datetime

################################## CONSTANTS ##################################

MODULE_NAME = "obcache"

################################### GLOBALS ###################################

L = None

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

################################### HELPERS ###################################

def split_message(msg_parts):
    separator_idx = None
    for i, part in enumerate(msg_parts):
        if not part:
            separator_idx = i
            break
    if not separator_idx:
        raise ValueError("ident separator not found")
    ident = msg_parts[:separator_idx]
    msg = msg_parts[separator_idx+1]
    return ident, msg

def ident_to_str(ident):
    return "/".join([x.decode("latin-1").replace("/", "\/") for x in ident])
    
###############################################################################

class OrderBook:

    def __init__(self, ticker_id, f2i_converter, i2f_converter, float_volume):
        self.f2i_conv = f2i_converter
        self.i2f_conv = i2f_converter
        # descending order
        self._bids = SortedDict(lambda x: -x)
        self._implied_bids = SortedDict(lambda x: -x)
        self._asks = SortedDict()
        self._implied_asks = SortedDict()
        self._max_bid = None
        self._min_ask = None
        self._ticker_id = ticker_id
        if float_volume:
            self._zero_volume = 0.0
        else:
            self._zero_volume = 0
        self.initialized = False

    def update(self, data):
        updates = {
            "bids": {},
            "asks": {},
            "implied_bids": {},
            "implied_asks": {},
        }
        self._update_book(data, "bids", updates)
        self._update_book(data, "implied_bids", updates)
        self._update_book(data, "asks", updates)
        self._update_book(data, "implied_asks", updates)
        self._update_bbo_and_sanitize(data, updates)
        self._convert_update_book_to_list("bids", updates, True)
        self._convert_update_book_to_list("implied_bids", updates, True)
        self._convert_update_book_to_list("asks", updates, False)
        self._convert_update_book_to_list("implied_asks", updates, False)
        self._maybe_flag_initialized()
        return updates

    def _maybe_flag_initialized(self):
        if self.initialized:
            return
        if self._bids and self._asks:
            self.initialized = True

    # mutates updates
    def _update_book(self, data, book_name, updates):
        levels = data.get(book_name)
        if not levels:
            return
        attr_name = "_" + book_name
        book = self.__dict__.get(attr_name)
        upd_book = updates[book_name]
        for lvl in levels:
            price_raw, size = lvl
            price = self.f2i_conv(price_raw)
            old_size = book.get(price)
            # don't emit updates that actually change nothing
            if old_size == size:
                continue
            if size == 0:
                # L.debug("del {}".format(self.i2f_conv(price)))
                try:
                    del book[price]
                except KeyError:
                    pass
            else:
                book[price] = size
            upd_book[price] = size

    # mutates updates
    def _convert_update_book_to_list(self, book_name, updates, reverse):
        upd_book = updates[book_name]
        res = [[self.i2f_conv(p), s] for p, s in sorted(upd_book.items())]
        if reverse:
            res = res[::-1]
        updates[book_name] = res

    # mutates updates
    def _update_bbo_and_sanitize(self, data, updates):
        # Aggression values are used to implement sweeping logic when
        # max_bid/min_ask overlap.
        bid_aggression, ask_aggression = self._update_bbo(data)
        # check for overlapping levels
        if self._min_ask is not None and self._max_bid is not None \
                and self._max_bid >= self._min_ask:
            L.debug("bid_aggr: {}, ask_aggr: {}"
                    .format(bid_aggression, ask_aggression))
            if ask_aggression <= 0 and bid_aggression <= 0:
                L.critical("max_bid/min_ask are overlapping and aggression "
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
                min_ask = sys.maxsize
                min_ask = min(min_ask,
                              self._sweep_book("asks", False, updates))
                min_ask = min(min_ask,
                              self._sweep_book("implied_asks", False, updates))
                self._min_ask = min_ask if min_ask < sys.maxsize else None
            else:
                max_bid = -sys.maxsize
                max_bid = max(max_bid,
                              self._sweep_book("bids", True, updates))
                max_bid = max(max_bid,
                              self._sweep_book("implied_bids", True, updates))
                self._max_bid = max_bid if max_bid > -sys.maxsize else None

    def _repr_bbo(self):
        s = "{}: bbo={}/{}"
        s = s.format(self._ticker_id,
                     self.i2f_conv(self._max_bid) if self._max_bid else None,
                     self.i2f_conv(self._min_ask) if self._min_ask else None)
        if self._max_bid is not None and self._min_ask is not None:
            s += " ({:.5%})".format(self._min_ask / self._max_bid - 1)
        return s

    def _update_bbo(self, data):
        # Update min_ask and max_bid by comparing data with old min_ask and
        # min_bid values.
        # bids = data.get("bids")
        # implied_bids = data.get("implied_bids")
        # if data["bids"][0][1] == 0:
        changed = False
        max_bid = -sys.maxsize
        if self._bids:
            max_bid = max(max_bid, next(iter(self._bids)))
        if self._implied_bids:
            max_bid = max(max_bid, next(iter(self._implied_bids)))
        # if bids:
        #     max_bid = max(max_bid, self.f2i_conv(bids[0][0]))
        # if implied_bids:
        #     max_bid = max(max_bid, self.f2i_conv(implied_bids[0][0]))
        bid_aggression = 0
        if self._max_bid is None:
            if max_bid > -sys.maxsize:
                self._max_bid = max_bid
                # sys.maxsize is used here to express appearance of first bid.
                bid_aggression = sys.maxsize
                changed = True
        elif max_bid != self._max_bid:
            bid_aggression = max_bid - self._max_bid
            self._max_bid = max_bid
            changed = True
        # asks = data.get("asks")
        # implied_asks = data.get("implied_asks")
        min_ask = sys.maxsize
        if self._asks:
            min_ask = min(min_ask, next(iter(self._asks)))
        if self._implied_bids:
            min_ask = min(min_ask, next(iter(self._implied_asks)))
        # if asks:
        #     min_ask = min(min_ask, self.f2i_conv(asks[0][0]))
        # if implied_asks:
        #     min_ask = min(min_ask, self.f2i_conv(implied_asks[0][0]))
        ask_aggression = 0
        if self._min_ask is None:
            if min_ask < sys.maxsize:
                self._min_ask = min_ask
                # sys.maxsize is used here to express appearance of first ask.
                ask_aggression = sys.maxsize
                changed = True
        elif min_ask != self._min_ask:
            ask_aggression = self._min_ask - min_ask
            self._min_ask = min_ask
            changed = True
        if changed:
            L.debug(self._repr_bbo())
        return bid_aggression, ask_aggression

    # mutates updates
    def _sweep_book(self, book_name, is_bid, updates):
        L.debug("sweeping book: {}".format(book_name))
        attr_name = "_" + book_name
        book = self.__dict__.get(attr_name)
        upd_book = updates[book_name]
        for price in book:
            if is_bid and price >= self._min_ask:
                del book[price]
                upd_book[price] = self._zero_volume
            elif not is_bid and price <= self._max_bid:
                del book[price]
                upd_book[price] = self._zero_volume
            else:
                return price
        return -sys.maxsize if is_bid else sys.maxsize

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
    if msg["result"] != "ok":
        return None
    return msg["content"]

def get_converters(msg):
    if msg["float_price"]:
        ts = msg["price_tick_size"]
        if isinstance(ts, list):
            # use the smallest tick size
            ts = tick_size[0][1]
        num_decimals = len(str(ts).split(".")[1])
        ts = 1.0 * 10 ** -num_decimals
        def f2i_converter(x):
            return round(x / ts)
        def i2f_converter(x):
            return round(x * ts, num_decimals)
    else:
        # If prices are in int format no conversion needed =>
        # return identity functions.
        def f2i_converter(x):
            return x
        def i2f_converter(x):
            return x
    return f2i_converter, i2f_converter

# mutates data
async def convert_data(ticker_id: bytes, data):
    cache = g.cache.get(ticker_id)
    if not cache:
        g.cache[ticker_id] = cache = {}
        ticker_id_str = ticker_id.decode()
        L.info("fetching ticker info for {} ...".format(ticker_id_str))
        # if R is None:
        #     msg = "variable tick_size is not implemented yet, ignored {}"
        #     msg = msg.format(ticker_id)
        #     L.warning(msg)
        #     topic = "{}\x03".format(MODULE_NAME)
        #     msg = "variable ticksize is not implemented, ignored: 
        #     g.sock_pub.send_multipart([topic.encode(), msg.encode()])
        #     g.cache[ticker_id] = 0
        #     return data
        ticker_info = await get_ticker_info(ticker_id_str)
        if not ticker_info:
            L.warning("failed to fetch ticker_info for {}"
                      .format(ticker_id_str))
            g.cache[ticker_id] = 0
            return data
        f2i_converter, i2f_converter = get_converters(ticker_info)
        L.info("price converter constructed for: {}".format(ticker_id_str))
        cache["order_book"] = OrderBook(ticker_id,
                                        f2i_converter,
                                        i2f_converter,
                                        ticker_info["float_volume"])
    # L.debug(data["bids"][0])
    # print("INPUT")
    # pprint(data)
    ob = cache["order_book"]
    updates = ob.update(data)
    # print("OUTPUT")
    # pprint(updates)
    if updates["bids"]:
        data["bids"] = updates["bids"]
    if updates["implied_bids"]:
        data["implied_bids"] = updates["implied_bids"]
    if updates["asks"]:
        data["asks"] = updates["asks"]
    if updates["implied_asks"]:
        data["implied_asks"] = updates["implied_asks"]

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
    await convert_data(ticker_id, data)
    msg = (" " + json.dumps(data)).encode()
    return msg

async def handle_md_msg(ticker_id: bytes, msg_parts):
    g.pub_bytes_in += sum([len(x) for x in msg_parts])
    topic = msg_parts[0]
    try:
        msg_bytes = await convert_md_msg(ticker_id, msg_parts[1])
    except Exception as err:
        L.exception("exception when converting message:", err)
    else:
        g.pub_bytes_out += len(topic) + len(msg_bytes)
        await g.sock_pub.send_multipart([topic, msg_bytes])

async def run_md_converter():
    L.info("running md converter coroutine...")
    while True:
        msg_parts = await g.sock_sub.recv_multipart()
        msg_type = msg_parts[0][-1]
        if msg_type != 1:
            await g.sock_pub.send_multipart(msg_parts)
            continue
        ticker_id = msg_parts[0][:-1]
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

def construct_ob_levels_from_cache(cache, num_levels):
    ob = cache["order_book"]
    def handle_book(book):
        res = []
        for i, lvl in enumerate(book.items()):
            if i == num_levels:
                break
            price_i, size = lvl
            price_f = ob.i2f_conv(price_i)
            res.append([price_f, size])
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

async def get_status(ident, msg):
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

async def handle_ctl_msg_1(ident, msg_raw):
    msg = json.loads(msg_raw.decode())
    msg_id = msg["msg_id"]
    cmd = msg["command"]
    debug_msg = "ident={}, command={}".format(ident_to_str(ident), cmd)
    L.debug("> " + debug_msg)
    try:
        if cmd == "get_snapshot":
            await handle_get_snapshot(ident, msg)
        elif cmd == "get_status":
            await get_status(ident, msg)
        else:
            await fwd_message_no_change(msg_id, ident + [b"", msg_raw])
    except Exception as e:
        L.exception("exception on msg_id: {}".format(msg_id))
        await send_error(ident, msg_id, error.GENERIC, str(e))
    L.debug("< " + debug_msg)

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
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args

def build_logger(args):
    logging.root.setLevel(args.log_level)
    logger = logging.getLogger(__name__)
    logger.propagate = False
    logger.handlers.clear()
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    # convert datetime to utc
    formatter.converter = gmtime
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

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

async def check_capabilities():
    # meant to be run only at the initialization phase
    data = { "command": "list_capabilities" }
    msg = " " + json.dumps(data)
    msg = msg.encode()
    await g.sock_deal.send_multipart([b"", msg])
    msg_parts = await g.sock_deal.recv_multipart()
    data = json.loads(msg_parts[1].decode())
    if data["result"] != "ok" or \
            "GET_TICKER_INFO_PRICE_TICK_SIZE" not in data["content"]:
        L.critical("MD does not support GET_TICKER_INFO_PRICE_TICK_SIZE cap")
        sys.exit(1)

def main():
    global L
    args = parse_args()
    L = build_logger(args)
    init_zmq_sockets(args)
    L.info("checking md capabilities ...")
    g.loop.run_until_complete(check_capabilities())
    L.info("md capabilities ok")
    tasks = [
        create_task(run_md_converter()),
        create_task(run_ctl_interceptor()),
        create_task(g.sock_deal_pub.run()),
    ]
    g.loop.run_until_complete(asyncio.gather(*tasks))


if __name__ == "__main__":
    main()
