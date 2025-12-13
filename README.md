# High-Performance Cryptocurrency Matching Engine

This project is a high-performance cryptocurrency matching engine developed in Python. It implements core trading functionalities based on REG NMS-inspired principles of **price-time priority** and **internal order protection**. The engine is built using FastAPI and `sortedcontainers` for high-throughput and low-latency order processing.

## ✨ Features
-   **Price-Time Priority Matching**: Higher bids and lower asks are always prioritized. At the same price level, orders are filled based on their arrival time (FIFO).
-   **Internal Trade-Through Protection**: Incoming marketable orders are always matched at the best available price(s) on the internal order book.
-   **Core Order Types**: Supports **Market**, **Limit**, **Immediate-Or-Cancel (IOC)**, and **Fill-Or-Kill (FOK)** orders.
-   **Real-time Data Dissemination**: Uses WebSockets to stream live trade executions and L2 order book updates.
-   **High-Performance Data Structures**: Utilizes `sortedcontainers.SortedDict` for the order book, providing logarithmic time complexity ($O(\log N)$) for most operations.

---

## 🏗️ System Architecture

The engine's architecture is designed for performance and clarity.

* **API Layer (FastAPI)**: Provides REST endpoints for order submission and WebSocket endpoints for real-time data streams. It handles all network I/O asynchronously.
* **Core Engine (`MatchingEngine`)**: A synchronous, thread-safe core that processes one order at a time to ensure data integrity. It contains the matching logic.
* **Order Book (`OrderBook`)**: The central data structure.
    * `SortedDict` is used to maintain price-sorted levels.
    * `collections.deque` is used at each price level to maintain time priority (FIFO).
* **Decoupling with Queues**: `asyncio.Queue` is used to pass data (trades, book updates) from the synchronous matching core to the asynchronous broadcasting tasks, preventing the core logic from being blocked by network operations.



---

## 🚀 Getting Started

### Prerequisites
-   Python 3.10+
-   `pip` and `venv`
-   A command-line WebSocket client like [`websocat`](https://github.com/vi/websocat).
-   `curl` for sending HTTP requests.

### 1. Setup
Clone the repository or create the files locally. Then, set up a virtual environment and install the dependencies.

```bash
# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
