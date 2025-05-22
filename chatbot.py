import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Set
import websockets
from websockets.server import WebSocketServer, serve
from websockets.exceptions import ConnectionClosedError, InvalidMessage
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ServerConfig:
    host: str = '0.0.0.0'
    port: int = int(os.environ.get("PORT", 10000))
    max_attempts: int = 20
    cleanup_interval: int = 15
    connection_timeout: int = 15
    max_message_size: int = 2**20  # 1MB
    ping_interval: int = 20
    ping_timeout: int = 10
    allowed_origins: Set[str] = None

    def __post_init__(self):
        if self.allowed_origins is None:
            # Allow all origins in development, restrict in production
            self.allowed_origins = {'*'} if os.environ.get('ENVIRONMENT') != 'production' else {
                'https://chatbot-frontend-xqiy-1roh5fo8f-rrs-projects-de5f63ae.vercel.app/',
                'https://chatbot-frontend-l9qs.vercel.app/'
                'http://localhost:3000'  # Local development
            }

class ChatServer:
    def __init__(self, config: Optional[ServerConfig] = None):
        self.config = config or ServerConfig()
        self.clients: dict[WebSocketServer, str] = {}
        self.rooms: dict[str, Set[WebSocketServer]] = {"general": set()}
        self.lock = asyncio.Lock()
        self.connection_attempts: dict[str, list[datetime]] = {}
        self.ban_list: dict[str, datetime] = {}

    async def cleanup_old_attempts(self):
        """Clean up connection attempts older than 15 seconds."""
        current_time = datetime.now()
        for ip in list(self.connection_attempts.keys()):
            self.connection_attempts[ip] = [
                attempt for attempt in self.connection_attempts[ip]
                if (current_time - attempt).total_seconds() < 15
            ]
            if not self.connection_attempts[ip]:
                del self.connection_attempts[ip]

    async def process_request(self, path, request_headers):
        """Handle incoming connections with rate limiting and CORS."""
        try:
            logger.info(f"Processing request from {request_headers.get('Origin', 'unknown')}")
            
            # Handle CORS preflight
            if request_headers.get('Sec-WebSocket-Protocol'):
                origin = request_headers.get('Origin', '')
                if '*' in self.config.allowed_origins or origin in self.config.allowed_origins:
                    return None
                logger.warning(f"Blocked connection from disallowed origin: {origin}")
                return 403, [], b"Origin not allowed"

            # Handle WebSocket upgrade request
            if request_headers.get('Upgrade', '').lower() != 'websocket':
                logger.warning("Non-WebSocket connection attempt")
                return 426, [], b"Upgrade Required"

            client_ip = request_headers.get('X-Forwarded-For', 'unknown')
            logger.info(f"Connection attempt from {client_ip}")
            
            # Check if IP is banned
            if client_ip in self.ban_list:
                ban_time = self.ban_list[client_ip]
                # Auto-unban after 1 minute
                if (datetime.now() - ban_time).total_seconds() > 60:
                    del self.ban_list[client_ip]
                else:
                    logger.warning(f"Blocked banned IP: {client_ip}")
                    return 403, [], b"IP banned"

            await self.cleanup_old_attempts()
            
            self.connection_attempts.setdefault(client_ip, [])
            
            # Only count attempts in the last 15 seconds
            recent_attempts = [
                attempt for attempt in self.connection_attempts[client_ip]
                if (datetime.now() - attempt).total_seconds() < 15
            ]
            
            if len(recent_attempts) >= self.config.max_attempts:
                self.ban_list[client_ip] = datetime.now()
                logger.warning(f"Banned {client_ip} for excessive attempts")
                return 429, [], b"Too many connection attempts"
            
            self.connection_attempts[client_ip].append(datetime.now())
            return None
        except Exception as e:
            logger.error(f"Request processing error: {e}", exc_info=True)
            return None

    async def handle_client(self, websocket: WebSocketServer):
        """Manage client connection lifecycle."""
        # Verify origin before proceeding
        origin = websocket.request_headers.get('Origin', '')
        logger.info(f"New WebSocket connection from {origin}")
        
        if '*' not in self.config.allowed_origins and origin not in self.config.allowed_origins:
            logger.warning(f"Blocked connection from disallowed origin: {origin}")
            await websocket.close(1008, "Origin not allowed")
            return

        if websocket in self.clients:
            logger.warning("Duplicate connection from existing socket")
            return

        username = None
        try:
            # Initial connection setup
            message = await asyncio.wait_for(
                websocket.recv(),
                timeout=self.config.connection_timeout
            )
            data = json.loads(message)
            logger.info(f"Received initial message: {data}")
            
            if data.get("type") != "connect":
                raise ValueError("First message must be 'connect'")

            username = data.get("username")
            if not username:
                raise ValueError("Username required")

            if username in self.clients.values():
                await websocket.send(json.dumps({
                    "type": "error",
                    "content": "Username taken"
                }))
                return

            # Register client
            async with self.lock:
                self.clients[websocket] = username
                self.rooms["general"].add(websocket)

            # Notify system
            welcome_msg = {
                "type": "system",
                "content": f"Welcome {username}!",
                "timestamp": datetime.now().isoformat()
            }
            await websocket.send(json.dumps(welcome_msg))
            await self.broadcast({
                "type": "system",
                "content": f"{username} joined",
                "timestamp": datetime.now().isoformat()
            })
            await self.broadcast_user_list()

            # Main message loop
            async for message in websocket:
                try:
                    await self.handle_message(websocket, username, message)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON message from {username}")
                    continue
                except Exception as e:
                    logger.error(f"Error handling message from {username}: {e}")
                    continue

        except (asyncio.TimeoutError, json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Connection setup failed: {type(e).__name__}: {e}")
        except ConnectionClosedError:
            logger.info(f"Client disconnected: {username}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            await self.cleanup_client(websocket, username)

    async def handle_message(self, websocket: WebSocketServer, username: str, message: str):
        """Process incoming messages."""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            logger.info(f"Received message from {username}: {data}")

            if msg_type == "message":
                room = data.get("room", "general")
                if room not in self.rooms:
                    self.rooms[room] = set()
                
                message_data = {
                    "type": "message",
                    "content": data["content"],
                    "sender": username,
                    "room": room,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Send to all clients in the room
                await self.broadcast(message_data, room=room)
                logger.info(f"Broadcast message from {username} in {room}: {data['content']}")

            elif msg_type == "private":
                target = next(
                    (c for c, name in self.clients.items() if name == data["to"]),
                    None
                )
                if target:
                    private_msg = {
                        "type": "private",
                        "from": username,
                        "to": data["to"],
                        "content": data["content"],
                        "timestamp": datetime.now().isoformat()
                    }
                    await asyncio.gather(
                        target.send(json.dumps(private_msg)),
                        websocket.send(json.dumps(private_msg))
                    )
                    logger.info(f"Private message from {username} to {data['to']}")
                else:
                    error_msg = {
                        "type": "error",
                        "content": f"User {data['to']} not found",
                        "timestamp": datetime.now().isoformat()
                    }
                    await websocket.send(json.dumps(error_msg))
            
            elif msg_type == "ping":
                await websocket.send(json.dumps({
                    "type": "pong",
                    "timestamp": datetime.now().isoformat()
                }))

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON message from {username}")
            raise
        except Exception as e:
            logger.error(f"Error processing message from {username}: {e}")
            raise

    async def cleanup_client(self, websocket: WebSocketServer, username: Optional[str]):
        """Clean up after disconnected client."""
        if not username:
            return

        async with self.lock:
            self.clients.pop(websocket, None)
            for room in self.rooms.values():
                room.discard(websocket)

            await self.broadcast({
                "type": "system",
                "content": f"{username} left",
                "timestamp": datetime.now().isoformat()
            })
            await self.broadcast_user_list()

    async def broadcast_user_list(self):
        """Send updated user list to all clients."""
        await self.broadcast({
            "type": "user_list",
            "users": list(self.clients.values()),
            "timestamp": datetime.now().isoformat()
        })

    async def broadcast(self, message: dict, room: str = "general", exclude: Optional[WebSocketServer] = None):
        """Send message to all clients in a room."""
        if room not in self.rooms:
            logger.warning(f"Room {room} does not exist")
            return

        message_str = json.dumps(message)
        tasks = []
        for client in list(self.rooms[room]):
            if client != exclude and client.open:
                try:
                    tasks.append(client.send(message_str))
                    logger.debug(f"Queued message for client in {room}")
                except Exception as e:
                    logger.error(f"Error sending message to client: {e}")
                    continue
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error in broadcast: {result}")

    async def start(self):
        """Start the WebSocket server."""
        async with serve(
            self.handle_client,
            self.config.host,
            self.config.port,
            ping_interval=self.config.ping_interval,
            ping_timeout=self.config.ping_timeout,
            max_size=self.config.max_message_size,
            process_request=self.process_request,
            origins=self.config.allowed_origins
        ):
            logger.info(f"Server running on ws://{self.config.host}:{self.config.port}")
            await asyncio.Future()  # Run forever

if __name__ == "__main__":
    server = ChatServer()
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.critical(f"Server crash: {e}", exc_info=True)
