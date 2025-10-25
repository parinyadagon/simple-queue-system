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
	conns map[*websocket.Conn]bool
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
	n.RLock()
	defer n.RUnlock()

	for conn := range n.conns {
		if err := conn.WriteJSON(job); err != nil {
			log.Println("Write error:", err)
			// (ตวรมี logic ลบ conn ที่ error ออก)
			n.Lock()
			delete(n.conns, conn)
			n.Unlock()
			conn.Close()

			continue
		}
	}
}
