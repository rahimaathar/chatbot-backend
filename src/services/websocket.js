import { EventEmitter } from 'events';

class WebSocketService extends EventEmitter {
    constructor() {
        super();
        this.socket = null;
        this.isConnected = false;
        this.isConnecting = false; // Flag to prevent concurrent connection attempts
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000; // Start with 1 second
        this.maxReconnectDelay = 30000; // Max 30 seconds
        this.connectionTimeout = 10000; // 10 seconds
        this.heartbeatInterval = 30000; // 30 seconds
        this.heartbeatTimer = null;
        this.reconnectTimer = null;
        this.lastMessageTime = Date.now();
        this.messageQueue = [];
        this.isProcessingQueue = false;
        this.connectionPromise = null;
        this.connectionPromiseResolve = null;
        this.connectionPromiseReject = null;
        this.currentUsername = null; // Store the username for reconnections
    }

    connect(usernameParam) { // Renamed param for clarity
        if (this.socket && this.isConnected) {
            console.log('WebSocket already connected');
            // If a promise exists from a previous attempt that is now resolved by being connected
            if (this.connectionPromiseResolve) this.connectionPromiseResolve();
            return Promise.resolve();
        }

        if (this.isConnecting && this.connectionPromise) {
            console.log('Connection already in progress');
            return this.connectionPromise;
        }

        this.isConnecting = true;

        // Update stored username if a new valid username is provided for this attempt
        if (typeof usernameParam === 'string' && usernameParam.trim() !== '') {
            this.currentUsername = usernameParam;
        }

        // Use the stored username for this connection attempt
        const userToConnectAs = this.currentUsername;

        if (!userToConnectAs) {
            const errMsg = 'WebSocketService: Username not available for connection attempt.';
            console.error(errMsg);
            this.isConnecting = false; // Reset flag
            this.emit('error', new Error(errMsg));
            return Promise.reject(new Error(errMsg)); // Return a rejected promise
        }

        // Setup new promise for this connection attempt
        this.connectionPromise = new Promise((resolve, reject) => {
            this.connectionPromiseResolve = resolve;
            this.connectionPromiseReject = reject;
        });

        try {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            console.log(`Connecting to WebSocket server as ${userToConnectAs}:`, wsUrl);

            this.socket = new WebSocket(wsUrl);
            this.setupSocketHandlers(userToConnectAs); // Pass the username for this specific connection
            this.startConnectionTimeout();
        } catch (error) {
            console.error('Error creating WebSocket connection:', error);
            this.isConnecting = false; // Reset flag on error
            // handleConnectionError will be called by socket.onerror or socket.onclose if instantiation fails
            // but if new WebSocket() itself throws, we need to handle it here.
            if (this.connectionPromiseReject) {
                this.connectionPromiseReject(error);
            }
            this.handleConnectionError(error); // Ensure this is robust
        }

        return this.connectionPromise;
    }

    setupSocketHandlers(usernameForConnectMessage) {
        this.socket.onopen = () => {
            console.log('WebSocket connection opened');
            this.isConnected = true;
            this.isConnecting = false; // Clear connecting flag
            this.reconnectAttempts = 0; // Reset on successful connection
            this.reconnectDelay = 1000; // Reset exponential backoff delay

            if (this.connectionPromiseResolve) {
                this.connectionPromiseResolve();
            }
            this.clearConnectionPromise(); // Clear promise fields after resolution

            this.startHeartbeat();
            this.sendConnectMessage(usernameForConnectMessage);
            this.processMessageQueue();
        };

        this.socket.onclose = (event) => {
            console.log('WebSocket connection closed:', event.code, event.reason);
            const wasConnected = this.isConnected;
            this.isConnected = false;
            this.isConnecting = false; // Clear connecting flag
            this.stopHeartbeat();
            // Avoid calling handleConnectionError if it was an intentional disconnect (e.g. code 1000 and was connected)
            // or if already handling an error that led to close.
            // For now, let handleConnectionError decide if reconnect is needed.
            this.handleConnectionError(new Error(`Connection closed: ${event.code} ${event.reason}`));
        };

        this.socket.onerror = (error) => {
            console.error('WebSocket error:', error);
            this.isConnecting = false; // Clear connecting flag
            // Error often precedes close. Let onclose handle some states or handleConnectionError directly.
            this.handleConnectionError(error instanceof Error ? error : new Error('WebSocket error event'));
        };

        this.socket.onmessage = (event) => {
            try {
                this.lastMessageTime = Date.now();
                const data = JSON.parse(event.data);
                console.log('Received message:', data);
                this.emit('message', data);
            } catch (error) {
                console.error('Error parsing message:', error);
            }
        };
    }

    clearConnectionPromise() {
        this.connectionPromise = null;
        this.connectionPromiseResolve = null;
        this.connectionPromiseReject = null;
    }

    startConnectionTimeout() {
        // Ensure no old timeout is running
        if (this.connectionTimeoutTimer) clearTimeout(this.connectionTimeoutTimer);

        this.connectionTimeoutTimer = setTimeout(() => {
            if (!this.isConnected && this.isConnecting) { // Only if attempting to connect
                console.error('Connection timeout');
                this.isConnecting = false;
                this.handleConnectionError(new Error('Connection timeout'));
                if (this.socket) {
                    this.socket.close(1005, 'Connection timeout'); // 1005 is No Status Rcvd
                }
            }
        }, this.connectionTimeout);
    }

    startHeartbeat() {
        this.stopHeartbeat(); // Clear any existing heartbeat
        this.heartbeatTimer = setInterval(() => {
            if (this.isConnected && this.socket && this.socket.readyState === WebSocket.OPEN) {
                this.send({ type: 'ping' });
            } else if (!this.isConnected && !this.isConnecting) {
                // If not connected and not trying to connect, attempt to reconnect
                console.log('Heartbeat: Detected disconnected state, attempting reconnect.');
                this.scheduleReconnect();
            }
        }, this.heartbeatInterval);
    }

    stopHeartbeat() {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    handleConnectionError(error) {
        // Avoid multiple entries if error is rapidly followed by close
        if (!this.isConnected && !this.isConnecting && this.reconnectAttempts > 0) {
            // console.log("Already handling a disconnected state or reconnect sequence, skipping redundant error handling for:", error.message);
            // return; // This might be too aggressive, let's refine.
        }

        console.error('Handling connection error:', error.message);
        this.isConnected = false;
        this.isConnecting = false; // Ensure this is reset
        this.stopHeartbeat();

        if (this.connectionPromiseReject) {
            this.connectionPromiseReject(error);
        }
        this.clearConnectionPromise(); // Clear promise fields as connection attempt failed

        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.scheduleReconnect();
        } else {
            console.error('Max reconnection attempts reached. Will not attempt further reconnections.');
            this.emit('error', new Error('Max reconnection attempts reached. Please try connecting manually later.'));
        }
    }

    scheduleReconnect() {
        if (this.isConnecting || (this.socket && this.socket.readyState === WebSocket.OPEN)) {
            // Already connecting or connected, don't schedule another reconnect.
            return;
        }
        if (this.reconnectTimer) { // Prevent multiple concurrent reconnect timers
            clearTimeout(this.reconnectTimer);
        }

        this.reconnectAttempts++;
        const delay = Math.min(this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1), this.maxReconnectDelay);
        // Update reconnectDelay for next potential failure before this attempt
        this.reconnectDelay = delay;

        console.log(`Scheduling reconnect attempt ${this.reconnectAttempts} as ${this.currentUsername} in ${delay}ms`);

        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (this.currentUsername) {
                console.log('Attempting to reconnect...');
                this.connect(this.currentUsername); // connect will set isConnecting=true
            } else {
                console.error("WebSocketService: Cannot reconnect, no username stored.");
                this.emit('error', new Error('Cannot reconnect: username not available.'));
            }
        }, delay);
    }

    sendConnectMessage(username) {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            console.error('Cannot send connect message: socket not open. State:', this.socket ? this.socket.readyState : 'null');
            // Queue it or handle error. For now, log and potentially trigger error handling.
            // This scenario should ideally be prevented by checks before calling sendConnectMessage.
            return;
        }

        try {
            const connectMessage = {
                type: 'connect',
                username: username
            };
            this.socket.send(JSON.stringify(connectMessage));
            console.log('Sent connect message:', connectMessage);
        } catch (error) {
            console.error('Error sending connect message:', error);
            this.handleConnectionError(error); // This might trigger a reconnect cycle if called during connection setup
        }
    }

    send(message) {
        if (!this.isConnected || !this.socket || this.socket.readyState !== WebSocket.OPEN) {
            console.log('Not connected, queueing message:', message);
            this.messageQueue.push(message);
            // Do not call processMessageQueue here if not connected. It will be called on connect.
            // If not connected and not attempting to connect, maybe trigger a connect attempt.
            if (!this.isConnecting && !this.isConnected) {
                this.scheduleReconnect(); // Or this.connect(this.currentUsername) if immediate
            }
            return;
        }

        try {
            const messageStr = JSON.stringify(message);
            this.socket.send(messageStr);
            console.log('Sent message:', message);
        } catch (error) {
            console.error('Error sending message:', error);
            this.messageQueue.push(message); // Re-queue on send error
            this.handleConnectionError(error); // Treat send error as a connection issue
        }
    }

    async processMessageQueue() {
        if (this.isProcessingQueue || !this.messageQueue.length) {
            return;
        }

        this.isProcessingQueue = true;

        while (this.messageQueue.length > 0) {
            if (!this.isConnected || !this.socket || this.socket.readyState !== WebSocket.OPEN) {
                console.log('Cannot process queue, not connected.');
                break;
            }
            const message = this.messageQueue[0]; // Peek
            try {
                const messageStr = JSON.stringify(message);
                this.socket.send(messageStr);
                console.log('Sent queued message:', message);
                this.messageQueue.shift(); // Remove after successful send
            } catch (error) {
                console.error('Error sending queued message, will retry:', error);
                // Don't remove from queue, stop processing for now, error will be handled
                this.handleConnectionError(error);
                break;
            }
        }
        this.isProcessingQueue = false;
    }

    disconnect() {
        console.log('Disconnecting WebSocket explicitly...');
        if (this.reconnectTimer) { // Stop any scheduled reconnections
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        this.reconnectAttempts = 0; // Reset reconnect attempts for manual disconnect
        this.maxReconnectAttempts = 0; // Prevent automatic reconnections after explicit disconnect

        if (this.socket) {
            // Remove event listeners to prevent them from firing during explicit close
            this.socket.onopen = null;
            this.socket.onmessage = null;
            this.socket.onerror = null;
            this.socket.onclose = null;
            if (this.socket.readyState === WebSocket.OPEN || this.socket.readyState === WebSocket.CONNECTING) {
                this.socket.close(1000, 'User disconnected'); // 1000 is normal closure
            }
            this.socket = null;
        }
        this.isConnected = false;
        this.isConnecting = false;
        this.stopHeartbeat();
        this.messageQueue = [];
        this.isProcessingQueue = false;
        if (this.connectionTimeoutTimer) clearTimeout(this.connectionTimeoutTimer);

        // Do not clear this.currentUsername, so a subsequent manual connect() can reuse it
        // or Chat.js should clear its own username state to prevent using old credentials.
        this.clearConnectionPromise();
        console.log('WebSocket disconnected.');
        this.emit('close'); // Emit a general close event
    }
}

export const websocketService = new WebSocketService(); 