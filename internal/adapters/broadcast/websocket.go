package broadcast

import (
	"log"
	"simple-queue-103/internal/core/domain"
	"sync"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

type WebSocketNotifier struct {
	sync.RWMutex
	conns          map[*websocket.Conn]bool
	broadcastMutex sync.Mutex // Prevent concurrent broadcasts
}

func NewWebSocketNotifier() *WebSocketNotifier {
	return &WebSocketNotifier{
		conns: make(map[*websocket.Conn]bool),
	}
}

// HandleWS คือ Fiber Handler
func (n *WebSocketNotifier) HandleWS(c *fiber.Ctx) error {
	return websocket.New(func(k *websocket.Conn) {
		n.Lock()
		n.conns[k] = true
		n.Unlock()
		log.Println("New client connected")

		// Keep connection open
		for {
			if _, _, err := k.ReadMessage(); err != nil {
				log.Println("Client disconnected:", err)
				n.Lock()
				delete(n.conns, k)
				n.Unlock()
				break
			}
		}
	})(c)
}

// BroadcastUpdate (Implement ports.Notifier)
func (n *WebSocketNotifier) BroadcastUpdate(job *domain.Job) {
	// Prevent concurrent broadcasts to avoid "concurrent write to websocket connection"
	n.broadcastMutex.Lock()
	defer n.broadcastMutex.Unlock()

	n.RLock()
	// Create a copy of connections to avoid holding lock during write operations
	connsCopy := make([]*websocket.Conn, 0, len(n.conns))
	for conn := range n.conns {
		connsCopy = append(connsCopy, conn)
	}
	n.RUnlock()

	// Track connections that need to be removed
	var connsToDelete []*websocket.Conn

	// Write to connections without holding any locks
	for _, conn := range connsCopy {
		if err := conn.WriteJSON(job); err != nil {
			log.Println("Write error:", err)
			connsToDelete = append(connsToDelete, conn)

			// Close connection safely
			if closeErr := conn.Close(); closeErr != nil {
				log.Println("Error closing connection:", closeErr)
			}
		}
	}

	// Clean up failed connections
	if len(connsToDelete) > 0 {
		n.Lock()
		for _, conn := range connsToDelete {
			delete(n.conns, conn)
		}
		n.Unlock()
		log.Printf("Cleaned up %d disconnected WebSocket clients.", len(connsToDelete))
	}
}
