# Chat Application Backend

A robust WebSocket server implementation in Python that powers real-time chat functionality with advanced security features and room-based messaging.

## 🚀 Features

- **WebSocket Server**: Asynchronous WebSocket server using `websockets` library
- **Room Management**: Support for multiple chat rooms with dynamic creation
- **Private Messaging**: Direct user-to-user messaging capabilities
- **Security**:
  - Configurable CORS with wildcard pattern matching
  - Rate limiting (20 attempts per 15 seconds)
  - IP-based connection tracking
  - Automatic temporary bans for abuse
- **Connection Management**:
  - Automatic ping/pong for connection health
  - Graceful connection handling
  - User session tracking
- **Logging**: Comprehensive logging system for monitoring and debugging

## 🛠️ Tech Stack

- Python 3.8+
- `websockets` library
- `asyncio` for asynchronous operations
- Built-in `logging` module
- `fnmatch` for pattern matching

## 📋 Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

## ⚙️ Installation

1. Clone the repository:
```bash
git clone https://github.com/rahimaathar/chatbot-backend.git
cd chatbot-backend
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## 🔧 Configuration

The server can be configured through environment variables:

```bash
PORT=10000  # WebSocket server port
ENVIRONMENT=production  # Set to 'production' for strict CORS
```

Default configuration in `ServerConfig` class:
- Host: 0.0.0.0
- Port: 10000 (configurable via PORT env var)
- Max connection attempts: 20 per 15 seconds
- Connection timeout: 15 seconds
- Message size limit: 1MB
- Ping interval: 20 seconds
- Ping timeout: 10 seconds

## 🚀 Running the Server

```bash
python chatbot.py
```

The server will start on `ws://0.0.0.0:10000/ws` by default.

## 📡 WebSocket Protocol

### Connection

1. Connect to the WebSocket endpoint with the 'chat' subprotocol
2. Send initial connection message:
```json
{
  "type": "connect",
  "username": "your_username"
}
```

### Message Types

1. **Chat Message**
```json
{
  "type": "message",
  "content": "Hello, world!",
  "room": "general"
}
```

2. **Private Message**
```json
{
  "type": "private",
  "to": "recipient_username",
  "content": "Private message"
}
```

3. **Ping**
```json
{
  "type": "ping"
}
```

### System Messages

The server sends automatic system messages for:
- User joins/leaves
- Connection errors
- User list updates
- Room events

## 🔒 Security Features

### CORS Protection
- Configurable allowed origins
- Wildcard pattern support
- Strict mode in production

### Rate Limiting
- Tracks connection attempts per IP
- Maximum 20 attempts per 15 seconds
- Automatic temporary bans for violations

### Connection Security
- Message size limits
- Connection timeouts
- Ping/pong health checks
- Protocol validation

## 📝 Logging

The server implements comprehensive logging:
- Connection attempts
- Message processing
- Error handling
- Security events

Log format:
```
%(asctime)s - %(levelname)s - %(message)s
```

## 🧪 Testing

To run tests (when implemented):
```bash
python -m pytest tests/
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

