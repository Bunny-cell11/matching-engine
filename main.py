import asyncio
import datetime
import logging
import uuid
from collections import deque
from decimal import Decimal
from threading import Lock
from typing import Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, Field, validator
from sortedcontainers import SortedDict

# --- Basic Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI(title="REG NMS-inspired Matching Engine")

# --- Data Models (Pydantic) ---
class OrderIn(BaseModel):
    """Pydantic model for incoming order validation."""
    symbol: str
    order_type: str = Field(..., pattern="^(market|limit|ioc|fok)$")
    side: str = Field(..., pattern="^(buy|sell)$")
    quantity: Decimal = Field(..., gt=0)
    price: Optional[Decimal] = Field(None, gt=0)

    @validator('price', always=True)
    def price_required_for_limit(cls, v, values):
        if values.get('order_type') == 'limit' and v is None:
            raise ValueError('Price is required for limit orders')
        return v

class Order:
    """Internal representation of an order."""
    def __init__(self, user_id: str, order_in: OrderIn):
        self.order_id = str(uuid.uuid4())
        self.user_id = user_id
        self.symbol = order_in.symbol
        self.order_type = order_in.order_type
        self.side = order_in.side
        self.quantity = order_in.quantity
        self.price = order_in.price
        self.timestamp = datetime.datetime.now(datetime.timezone.utc)
        self.status = "open" # open, filled, canceled

class Trade:
    """Represents a single trade execution."""
    def __init__(self, symbol, price, quantity, aggressor_side, maker_order_id, taker_order_id):
        self.trade_id = str(uuid.uuid4())
        self.symbol = symbol
        self.timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z"
        self.price = price
        self.quantity = quantity
        self.aggressor_side = aggressor_side
        self.maker_order_id = maker_order_id
        self.taker_order_id = taker_order_id

    def to_dict(self):
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "trade_id": self.trade_id,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "aggressor_side": self.aggressor_side,
            "maker_order_id": self.maker_order_id,
            "taker_order_id": self.taker_order_id,
        }

# --- Core Matching Engine Components ---
class OrderBook:
    """
    Represents the order book for a single trading pair.
    - Bids are sorted from high to low price.
    - Asks are sorted from low to high price.
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        # For bids, we want to match highest price first. A negative key stores prices in descending order.
        self.bids = SortedDict(lambda k: -k)
        # For asks, we want to match lowest price first. The default SortedDict is ascending.
        self.asks = SortedDict()

    def get_bbo(self) -> Dict[str, Optional[Dict[str, str]]]:
        """Get the Best Bid and Offer."""
        best_bid = self.bids.peekitem(0)[0] if self.bids else None
        best_ask = self.asks.peekitem(0)[0] if self.asks else None

        bid_quantity = sum(order.quantity for order in self.bids[best_bid]) if best_bid else Decimal('0')
        ask_quantity = sum(order.quantity for order in self.asks[best_ask]) if best_ask else Decimal('0')

        return {
            "bid": {"price": str(best_bid), "quantity": str(bid_quantity)} if best_bid else None,
            "ask": {"price": str(best_ask), "quantity": str(ask_quantity)} if best_ask else None,
        }

    def get_depth(self, depth: int = 10) -> Dict[str, List[List[str]]]:
        """Get the order book depth."""
        bids_depth = []
        for price, orders in self.bids.items():
            if len(bids_depth) >= depth:
                break
            total_quantity = sum(o.quantity for o in orders)
            bids_depth.append([str(price), str(total_quantity)])

        asks_depth = []
        for price, orders in self.asks.items():
            if len(asks_depth) >= depth:
                break
            total_quantity = sum(o.quantity for o in orders)
            asks_depth.append([str(price), str(total_quantity)])

        return {"bids": bids_depth, "asks": asks_depth}

    def add_order(self, order: Order):
        """Add a limit order to the book."""
        book_side = self.bids if order.side == "buy" else self.asks
        if order.price not in book_side:
            book_side[order.price] = deque()
        book_side[order.price].append(order)
        logging.info(f"Order {order.order_id} added to {order.side} book at price {order.price}")

# --- REPLACEMENT FOR THE ENTIRE MatchingEngine CLASS ---
class MatchingEngine:
    def __init__(self):
        self.order_books: Dict[str, OrderBook] = {}
        self.lock = Lock()
        self.trade_queue = asyncio.Queue()
        self.book_update_queue = asyncio.Queue()

    def _get_or_create_book(self, symbol: str) -> OrderBook:
        if symbol not in self.order_books:
            self.order_books[symbol] = OrderBook(symbol)
        return self.order_books[symbol]

    async def process_order(self, order: Order):
        """Main entry point for processing an incoming order."""
        book_changed = False
        with self.lock:
            book = self._get_or_create_book(order.symbol)
            trades = []
            
            if order.order_type == 'market':
                trades = self._match_market_order(order, book)
            elif order.order_type == 'limit':
                trades = self._match_limit_order(order, book)
            elif order.order_type == 'ioc':
                # IOC logic is slightly different; it won't rest on the book.
                trades = self._match_ioc_order(order, book)
            elif order.order_type == 'fok':
                trades = self._match_fok_order(order, book)
            
            # If the order still has quantity after matching attempts (and it's a limit order), it rests on the book.
            if order.order_type == 'limit' and order.quantity > 0:
                book.add_order(order)
                book_changed = True # The book has definitely changed.

            # If trades occurred, the book also changed.
            if trades:
                book_changed = True
                for trade in trades:
                    await self.trade_queue.put(trade)
            
            # If the book state was altered in any way, publish an update.
            if book_changed:
                await self._publish_book_update(book)

    def _match_market_order(self, taker_order: Order, book: OrderBook) -> List[Trade]:
        trades = []
        book_side = book.asks if taker_order.side == "buy" else book.bids
        
        while taker_order.quantity > 0 and book_side:
            best_price_level = book_side.peekitem(0)
            orders_at_price = best_price_level[1]
            trades.extend(self._process_order_list(orders_at_price, taker_order))
            if not orders_at_price:
                book_side.popitem(0)

        if taker_order.quantity > 0:
            taker_order.status = "canceled"
        return trades

    def _match_limit_order(self, taker_order: Order, book: OrderBook) -> List[Trade]:
        trades = []
        book_side = book.asks if taker_order.side == "buy" else book.bids

        while taker_order.quantity > 0 and book_side:
            best_price_level = book_side.peekitem(0)
            price = best_price_level[0]
            orders_at_price = best_price_level[1]
            
            can_match = (taker_order.side == 'buy' and taker_order.price >= price) or \
                        (taker_order.side == 'sell' and taker_order.price <= price)

            if not can_match:
                break

            trades.extend(self._process_order_list(orders_at_price, taker_order))
            if not orders_at_price:
                book_side.popitem(0)
        
        return trades

    def _match_ioc_order(self, taker_order: Order, book: OrderBook) -> List[Trade]:
        # Behaves like a limit order but is never added to the book.
        trades = self._match_limit_order(taker_order, book)
        if taker_order.quantity > 0:
            taker_order.status = "canceled"
        return trades

    def _match_fok_order(self, taker_order: Order, book: OrderBook) -> List[Trade]:
        book_side = book.asks if taker_order.side == "buy" else book.bids
        
        available_quantity = Decimal('0')
        for price, orders in book_side.items():
            is_matchable_price = (taker_order.side == 'buy' and taker_order.price >= price) or \
                                 (taker_order.side == 'sell' and taker_order.price <= price)
            if is_matchable_price:
                available_quantity += sum(o.quantity for o in orders)
            if available_quantity >= taker_order.quantity:
                break
        
        if available_quantity < taker_order.quantity:
            taker_order.status = "canceled"
            logging.info(f"FOK Order {taker_order.order_id} could not be filled entirely. Canceled.")
            return []

        return self._match_limit_order(taker_order, book)

    def _process_order_list(self, orders: deque, taker_order: Order) -> List[Trade]:
        trades = []
        while orders and taker_order.quantity > 0:
            maker_order = orders[0]
            trade_quantity = min(taker_order.quantity, maker_order.quantity)

            trade = Trade(
                symbol=taker_order.symbol, price=maker_order.price, quantity=trade_quantity,
                aggressor_side=taker_order.side, maker_order_id=maker_order.order_id,
                taker_order_id=taker_order.order_id
            )
            trades.append(trade)

            maker_order.quantity -= trade_quantity
            taker_order.quantity -= trade_quantity

            if maker_order.quantity == 0:
                maker_order.status = "filled"
                orders.popleft()
        
        if taker_order.quantity == 0:
            taker_order.status = "filled"
        return trades

    async def _publish_book_update(self, book: OrderBook):
        update = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z",
            "symbol": book.symbol,
            **book.get_depth()
        }
        await self.book_update_queue.put(update)
        logging.info(f"Published book update for {book.symbol}")

# --- WebSocket Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, symbol: str):
        await websocket.accept()
        if symbol not in self.active_connections:
            self.active_connections[symbol] = []
        self.active_connections[symbol].append(websocket)
        logging.info(f"New WebSocket connection for {symbol}")

    def disconnect(self, websocket: WebSocket, symbol: str):
        self.active_connections[symbol].remove(websocket)
        if not self.active_connections[symbol]:
            del self.active_connections[symbol]
        logging.info(f"WebSocket disconnected for {symbol}")

    async def broadcast(self, message: dict, symbol: str):
        if symbol in self.active_connections:
            for connection in self.active_connections[symbol]:
                await connection.send_json(message)

# --- Global Instances ---
engine = MatchingEngine()
trade_manager = ConnectionManager()
book_manager = ConnectionManager()

# --- Background Tasks for Broadcasting ---
async def trade_broadcaster():
    """Listens to the trade queue and broadcasts trades."""
    while True:
        trade = await engine.trade_queue.get()
        logging.info(f"Broadcasting trade: {trade.trade_id}")
        await trade_manager.broadcast(trade.to_dict(), trade.symbol)

async def book_update_broadcaster():
    """Listens to the book update queue and broadcasts L2 data."""
    while True:
        update = await engine.book_update_queue.get()
        logging.info(f"Broadcasting book update for: {update['symbol']}")
        await book_manager.broadcast(update, update['symbol'])

@app.on_event("startup")
async def startup_event():
    # Start the background tasks
    asyncio.create_task(trade_broadcaster())
    asyncio.create_task(book_update_broadcaster())
    logging.info("Matching Engine and broadcasters are running.")

# --- API Endpoints ---
@app.post("/order", status_code=status.HTTP_202_ACCEPTED)
async def submit_order(order_in: OrderIn):
    """
    Endpoint to submit a new order.
    It returns immediately after accepting the order for processing.
    """
    # In a real system, user_id would come from an auth token
    user_id = "user-123" 
    order = Order(user_id=user_id, order_in=order_in)
    
    # Run matching in the background to not block the HTTP response
    asyncio.create_task(engine.process_order(order))
    
    return {"message": "Order accepted for processing", "order_id": order.order_id}

@app.websocket("/ws/trades/{symbol}")
async def websocket_trade_endpoint(websocket: WebSocket, symbol: str):
    await trade_manager.connect(websocket, symbol)
    try:
        while True:
            await websocket.receive_text() # Keep connection alive
    except WebSocketDisconnect:
        trade_manager.disconnect(websocket, symbol)

@app.websocket("/ws/book/{symbol}")
async def websocket_book_endpoint(websocket: WebSocket, symbol: str):
    await book_manager.connect(websocket, symbol)
    try:
        # Send initial book state upon connection
        book = engine._get_or_create_book(symbol)
        initial_update = {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z",
            "symbol": book.symbol,
            **book.get_depth()
        }
        await websocket.send_json(initial_update)
        
        while True:
            await websocket.receive_text() # Keep connection alive
    except WebSocketDisconnect:
        book_manager.disconnect(websocket, symbol)
