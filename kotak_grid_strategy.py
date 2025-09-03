""" Rohan
kotak_grid_socket.py

Kotak Neo Grid Strategy (similar to Zerodha version).
- Uses NeoAPI (REST) for login + order placement.
- Uses websocket-client for live bid/ask ticks.
- Grid-only execution: Buy only if ask <= grid; Sell only if bid >= target.
- Persists state to disk (grid_state.json).
"""

import json, os, sys, signal, time, threading, logging
from dataclasses import dataclass
from typing import Dict, Optional, List 
from neo_api_client import NeoAPI
import websocket 

# ---------------------------
# CONFIG
# ---------------------------

CONSUMER_KEY="OndGwjTfwqJYiV_fv5anD93OHGYa"
CONSUMER_SECRET="bcxHZQl8ny3B1ck19ERXy5YuJqAa"
API_USER_ID="XX5C5"
API_PASSWORD="Kuldip@0101"
API_MPIN="101010"
MOBILE = "8698474672"

SETTINGS = {
    "stock_name": "RELIANCE-EQ",
    "exchange_segment": "nse_cm",
    "position_size": 100,
    "buy_quantity": 5,
    "sell_quantity": 5,
    "buy_offset_or_interval": 5.0,
    "sell_offset_or_interval": 5.0,
    "buy_stock_range_from": 1300.0,
    "buy_stock_range_to": 1400.0,
    "sell_stock_range_from": 1300.0,
    "sell_stock_range_to": 1400.0,
    "buy_limit_buffer": 0,
    "sell_limit_buffer": 0,
    "tick_size": 0.05,
    "state_file": "grid_state.json",
    "min_processing_interval": 1.0
}

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("kotak_grid")
# logger.propagate = False   # prevent double logging


# ---------------------------
# State
# ---------------------------
@dataclass
class SymbolState:
    placed_buy_prices: set
    placed_sell_prices: set
    buy_sell_tracker: Dict[float, Dict[str, int]]
    last_processed: Optional[float]

    @staticmethod
    def empty():
        return SymbolState(set(), set(), {}, None)

def save_state(path: str, state: Dict[str, SymbolState]):
    tmp = {}
    for k, st in state.items():
        tmp[k] = {
            "placed_buy_prices": list(st.placed_buy_prices),
            "placed_sell_prices": list(st.placed_sell_prices),
            "buy_sell_tracker": {str(p): v for p, v in st.buy_sell_tracker.items()},
            "last_processed": st.last_processed
        }
    with open(path, "w") as f:
        json.dump(tmp, f)

def load_state(path: str) -> Dict[str, SymbolState]:
    if not os.path.exists(path): return {}
    with open(path) as f: raw = json.load(f)
    out = {}
    for k, st in raw.items():
        s = SymbolState.empty()
        s.placed_buy_prices = set(st.get("placed_buy_prices", []))
        s.placed_sell_prices = set(st.get("placed_sell_prices", []))
        s.buy_sell_tracker = {float(p): v for p, v in st.get("buy_sell_tracker", {}).items()}
        s.last_processed = st.get("last_processed")
        out[k] = s
    return out

def round_to_tick(price: float, tick: float) -> float:
    steps = round(price / tick)
    return round(steps * tick, 2)

# ---------------------------
# Grid Strategy
# ---------------------------
class KotakGridStrategy:
    def __init__(self, client: NeoAPI, settings: dict, access_token: str):
        self.client = client
        self.settings = settings
        self.access_token = access_token
        self.symbol_states = load_state(settings["state_file"])
        self.lock = threading.Lock()
        self.tick_size = settings["tick_size"]
        self.trading_symbol = settings["stock_name"]

        # Build grid levels
        low, high, step = settings["buy_stock_range_from"], settings["buy_stock_range_to"], settings["buy_offset_or_interval"]
        levels = []
        x = low
        while x <= high + 1e-9:
            levels.append(round_to_tick(x, self.tick_size))
            x += step
        self.grid_levels = sorted(set(levels))
        logger.info(f"Grid levels: {self.grid_levels}")

        self.last_ltp = None
        self.last_bid = None
        self.last_ask = None

    # ---------------------
    # Order placement
    # ---------------------
    def place_order(self, side: str, qty: int, limit_price: float):
        try:
            resp = self.client.place_order(
                exchange_segment=self.settings["exchange_segment"],
                product="MIS",
                price=str(limit_price),
                order_type="L",
                quantity=str(qty),
                validity="DAY",
                trading_symbol=self.trading_symbol,
                transaction_type="B" if side.lower()=="buy" else "S",
                amo="NO",
                disclosed_quantity="0",
                market_protection="0",
                pf="N",
                trigger_price="0"
            )
            logger.info(f"Placed {side.upper()} {qty} @ {limit_price} → resp={resp}")
            return True
        except Exception as e:
            logger.error(f"Order failed: {e}")
            return False

    # ---------------------
    # Tick handling
    # ---------------------
    def on_tick(self, ltp: Optional[float], bid_prices: List[float], ask_prices: List[float]):
        ss = self.symbol_states.setdefault(self.trading_symbol, SymbolState.empty())
        now = time.time()
        if ss.last_processed and now - ss.last_processed < self.settings["min_processing_interval"]:
            return
        ss.last_processed = now

        best_bid = max(bid_prices) if bid_prices else None
        best_ask = min(ask_prices) if ask_prices else None 

        logger.info(f"LTP={ltp}, BestBid={best_bid}, BestAsk={best_ask}")

        if best_ask: self.handle_buy(best_ask, ss)
        if best_bid: self.handle_sell(best_bid, ss)

        self.symbol_states[self.trading_symbol] = ss
        save_state(self.settings["state_file"], self.symbol_states)

    def handle_buy(self, best_ask, ss: SymbolState):
        for lvl in self.grid_levels:
            if lvl-0.5 <= best_ask <= lvl:
                tracker = ss.buy_sell_tracker.get(lvl, {"buy_qty":0,"sell_qty":0})
                if tracker["buy_qty"] > tracker["sell_qty"]:
                    return
                qty = min(self.settings["buy_quantity"], self.settings["position_size"])
                if self.place_order("buy", qty, lvl - self.settings["buy_limit_buffer"]):
                    tracker["buy_qty"] += qty
                    ss.buy_sell_tracker[lvl] = tracker
                break

    def handle_sell(self, best_bid, ss: SymbolState):
        for lvl, tracker in sorted(ss.buy_sell_tracker.items()):
            if tracker["buy_qty"] > tracker["sell_qty"]:
                target = round_to_tick(lvl + self.settings["sell_offset_or_interval"], self.tick_size)
                if target <= best_bid <= target+0.5:
                    sell_qty = min(self.settings["sell_quantity"], tracker["buy_qty"]-tracker["sell_qty"])
                    if sell_qty>0 and self.place_order("sell", sell_qty, target + self.settings["sell_limit_buffer"]):
                        tracker["sell_qty"] += sell_qty
                        ss.buy_sell_tracker[lvl] = tracker
                        break
    
    # First change
    def start_sdk_subscribe(self):
        """
        Use the neo_api_client SDK subscribe + callbacks instead of raw websocket.
        This avoids socket.io/protobuf handling issues.
        """
        # # friendly wrapper to parse SDK messages into on_tick(ltp, bids, asks)
        # def sdk_on_message(message):
        #     logger.debug(f"SDK on_message: type={type(message)} raw={str(message)[:300]}")
        #     try:
        #         # SDK often gives dict-like parsed messages. Try extracting common fields:
        #         payload = None
        #         if isinstance(message, dict):
        #             payload = message.get("data") or message
        #         elif isinstance(message, str):
        #             try:
        #                 payload = json.loads(message)
        #             except Exception:
        #                 payload = message
        #         else:
        #             # could be protobuf/bytes packaged by SDK; SDK usually decodes to dict, but be defensive
        #             payload = message

        #         if not payload or not isinstance(payload, dict):
        #             return

        #         # common heuristics (adjust if your feed uses different keys)
        #         ltp = payload.get("ltp") or payload.get("lastPrice") or payload.get("last_price") or payload.get("ltpc")
        #         depth = payload.get("depth") or payload.get("ff") or payload.get("depthInfo") or {}
        #         bids = []
        #         asks = []
        #         if isinstance(depth, dict):
        #             # depth may contain 'buy' and 'sell' arrays or nested fields
        #             buy = depth.get("buy", []) or depth.get("bids", []) or []
        #             sell = depth.get("sell", []) or depth.get("asks", []) or []
        #             try:
        #                 bids = [float(x.get("price") if isinstance(x, dict) else x) for x in buy]
        #                 asks = [float(x.get("price") if isinstance(x, dict) else x) for x in sell]
        #             except Exception:
        #                 bids, asks = [], []

        #         # finally hand off to your existing on_tick
        #         self.on_tick(ltp, bids, asks)
        #     except Exception as e:
        #         logger.exception("error parsing sdk message: %s", e)
        
        def sdk_on_message(message):
            try:
                logger.debug("RAW FEED: %s", message)

                if not isinstance(message, dict):
                    return

                # if message.get("type") != "stock_feed":
                #     return 

                for item in message.get("data", []):
                    if "ltp" in item:
                        self.last_ltp = float(item["ltp"])
                    if "bp1" in item:
                        self.last_bid = float(item["bp1"])
                    if "sp1" in item:
                        self.last_ask = float(item["sp1"])
                    if "depth" in item:
                        bids = [float(x["price"]) for x in item["depth"].get("buy", [])]
                        asks = [float(x["price"]) for x in item["depth"].get("sell", [])]
                        if bids: self.last_bid = bids[0]
                        if asks: self.last_ask = asks[0]

                    # Always log the latest snapshot
                    if self.last_ltp or self.last_bid or self.last_ask:
                        self.on_tick(
                            self.last_ltp,
                            [self.last_bid] if self.last_bid else [],
                            [self.last_ask] if self.last_ask else []
                        )
            except Exception as e:
                logger.exception("Error parsing SDK message: %s", e)

        # Attach callback handlers to client (SDK supports these hooks per README)
        self.client.on_message = sdk_on_message
        self.client.on_error = lambda e: logger.error("SDK on_error: %s", e)
        self.client.on_open = lambda m: logger.info("SDK websocket open: %s", m)
        self.client.on_close = lambda m: logger.warning("SDK websocket close: %s", m)

        # Resolve instrument token to the numeric token if possible (recommended)
        inst_tokens = []
        try:
            # try to find numeric instrument token via search_scrip (safe fallback)
            symbol_for_search = self.trading_symbol.split("-")[0]  # "RELIANCE" from "RELIANCE-EQ"
            # resp = self.client.search_scrip(
            #     exchange_segment=self.settings["exchange_segment"],
            #     symbol=self.trading_symbol.split("-")[0]
            # )
            # logger.debug("search_scrip response: %s", resp)

            # inst_tokens = []
            # if isinstance(resp, list) and resp:
            #     token = resp[0].get("pSymbol")
            #     if token:
            #         inst_tokens.append({
            #             "instrument_token": str(token),
            #             "exchange_segment": self.settings["exchange_segment"]
            #         })
            resp = self.client.search_scrip(
                exchange_segment=self.settings["exchange_segment"],
                symbol=self.trading_symbol.split("-")[0]  # e.g. "RELIANCE"
            )
            logger.debug("search_scrip response: %s", resp)

            inst_tokens = []
            if isinstance(resp, list) and resp:
                # Prefer the main equity group
                eq_entry = next((x for x in resp if x.get("pGroup") == "EQ"), None)
                if eq_entry:
                    token = eq_entry.get("pSymbol")
                    if token:
                        inst_tokens.append({
                            "instrument_token": str(token),
                            "exchange_segment": self.settings["exchange_segment"]
                        })


            # if not inst_tokens:
            #     raise RuntimeError(f"Could not resolve instrument token for {self.trading_symbol}")

            # logger.info("Subscribing (SDK) to: %s", inst_tokens)
            # self.client.subscribe(instrument_tokens=inst_tokens, isIndex=False, isDepth=True)

        except Exception as e:
            logger.debug("failed to resolve numeric token (ok to continue): %s", e)

        # fallback to textual subscribe if numeric token not resolved
        if not inst_tokens:
            # use feed-style key used in docs: "NSE_EQ|RELIANCE-EQ"
            inst_tokens = [{"instrument_token": f"NSE_EQ|{self.trading_symbol}", "exchange_segment": self.settings["exchange_segment"]}]

        logger.info("Subscribing (SDK) to: %s", inst_tokens)
        try:
            # subscribe for LTP
            self.client.subscribe(instrument_tokens=inst_tokens, isIndex=False, isDepth=False)

            # subscribe for depth
            self.client.subscribe(instrument_tokens=inst_tokens, isIndex=False, isDepth=True)
        except Exception as e:
            logger.exception("SDK subscribe failed: %s", e)

    # ---------------------
    # WebSocket
    # ---------------------
    # def start_websocket(self):
    #     url = "wss://mlhsm.kotaksecurities.com/feed"

    #     def on_open(ws):
    #         sub = {
    #             "guid": "rohan-001",
    #             "method": "sub",
    #             "data": {
    #                 "instrumentKeys": [f"NSE_EQ|{self.trading_symbol}"]
    #             }
    #         }
    #         ws.send(json.dumps(sub))
    #         logger.info(f"Subscribed {self.trading_symbol}")

    #     def on_message(ws, message):
    #         try:
    #             data = json.loads(message)
    #             print("Message: ", message)
    #             logger.debug(f"WS raw: {data}")   # full tick log
    #             if "data" in data:
    #                 d = data["data"]
    #                 ltp = d.get("ltp")
    #                 depth = d.get("depth", {})
    #                 bids = [float(b["price"]) for b in depth.get("buy",[])]
    #                 asks = [float(a["price"]) for a in depth.get("sell",[])]
    #                 self.on_tick(ltp, bids, asks)
    #         except Exception as e:
    #             print(e) 
    #             logger.error(f"WS parse error: {e}, msg={message}")

    #     ws = websocket.WebSocketApp(
    #         url,
    #         header=[f"Authorization: Bearer {self.access_token}"],
    #         on_open=on_open,
    #         on_message=on_message
    #     )
    #     ws.run_forever()
    def start_websocket(self):
        url = "wss://mlhsm.kotaksecurities.com/feed"

        # ensure Authorization header is correct (avoid "Bearer Bearer ..." if token already contains prefix)
        if isinstance(self.access_token, str) and self.access_token.lower().startswith("bearer"):
            auth_header = f"Authorization: {self.access_token}"
        else:
            auth_header = f"Authorization: Bearer {self.access_token}"

        def make_subscription_key():
            # Choose appropriate key format; SETTINGS["stock_name"] already has "RELIANCE-EQ"
            name = self.trading_symbol
            if "NIFTY" in name.upper() or "NIFTY 50" in name:
                return f"NSE_INDEX|{name}"
            # default to equity namespace
            return f"NSE_EQ|{name}"

        def on_open(ws):
            # logger.info("Websocket connected (on_open)")
            sub = {
                "guid": "rohan-001",
                "method": "sub",
                "data": {
                    "instrumentKeys": [make_subscription_key()]
                }
            }
            try:
                ws.send(json.dumps(sub))
                logger.info(f"Subscribed {self.trading_symbol}")
            except Exception as e:
                logger.error(f"Failed to send subscribe: {e}")

        def on_message(ws, message):
            # message can be bytes or str and may have socket.io prefixes like '42'
            try:
                logger.debug(f"RAW MESSAGE: {message[:200]}")
                # If parsing yields values:
                if payload and ltp:
                    logger.info(f"Tick: LTP={ltp} | Bids={bids[:1]} | Asks={asks[:1]}")

                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="ignore")

                # strip leading digits/socket.io numeric prefix until JSON starts
                if message and message[0].isdigit():
                    import re
                    m = re.search(r'(\[|\{)', message)
                    if m:
                        message = message[m.start():]

                data = json.loads(message)
            except Exception as e:
                logger.debug(f"Ignored non-JSON message or parse error: {e}, raw={str(message)[:200]}")
                return

            # message may be list (socket.io event style) or dict
            payload = None
            if isinstance(data, list):
                # e.g. ['eventName', { ... }]
                if len(data) >= 2 and isinstance(data[1], dict):
                    payload = data[1]
            elif isinstance(data, dict):
                payload = data.get("data") or data

            if not payload:
                return

            # normalize common fields (ltp + depth)
            ltp = payload.get("ltp") or payload.get("lastPrice") or None
            depth = payload.get("depth", {}) or {}
            # depth may contain 'buy' and 'sell' arrays
            bids = []
            asks = []
            try:
                bids = [float(b.get("price") if isinstance(b, dict) else b) for b in depth.get("buy", [])]
                asks = [float(a.get("price") if isinstance(a, dict) else a) for a in depth.get("sell", [])]
            except Exception:
                # fallback: try parsing any numeric elements
                try:
                    bids = [float(x) for x in depth.get("buy", [])]
                    asks = [float(x) for x in depth.get("sell", [])]
                except Exception:
                    bids = []
                    asks = []

            self.on_tick(ltp, bids, asks)

        def on_error(ws, err):
            logger.error(f"Websocket error: {err}")

        def on_close(ws, code, reason):
            logger.warning(f"Websocket closed. code={code}, reason={reason}")

        # Reconnect loop: recreate the app on each attempt so callbacks/Sub are fresh
        reconnect_delay = 1
        while True:
            ws = websocket.WebSocketApp(
                url,
                header=[auth_header],
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            try:
                # ping_interval sends a WebSocket ping every 20s; ping_timeout waits for pong
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                logger.exception(f"WS run_forever crashed: {e}")

            # after close, wait then retry (exponential backoff capped at 60s)
            logger.info(f"Websocket disconnected — reconnecting in {reconnect_delay}s ...")
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)


# ---------------------------
# Login helper 
# ---------------------------
def login_kotak():
    client = NeoAPI(
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        environment="prod"
    )

    logger.info("Logging in...")
    client.login(mobilenumber=MOBILE, password=API_PASSWORD)
    client.session_2fa(OTP=API_MPIN)
    logger.info("Login success")

    # We not subscribe here – WebSocket handle separately
    return client, client.configuration.bearer_token


# ---------------------------
# Main
# ---------------------------
def main():
    client, access_token = login_kotak()
    strat = KotakGridStrategy(client, SETTINGS, access_token)

    def sigint(sig, frame):
        logger.info("Stopping...")
        save_state(SETTINGS["state_file"], strat.symbol_states)
        sys.exit(0)
    signal.signal(signal.SIGINT, sigint)

    # strat.start_websocket()
    strat.start_sdk_subscribe()


if __name__=="__main__":
    main()
