package signal

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// frame 为信令帧统一封装：{"op": "...", "d": {...}}。
type frame struct {
	Op string `json:"op"`
	D  any    `json:"d"`
}

// wsConn 包装 websocket 连接：串行化写、幂等带因关闭；实现 room.Messenger。
type wsConn struct {
	mu     sync.Mutex
	conn   *websocket.Conn
	closed bool
}

const writeTimeout = 10 * time.Second

func newWSConn(c *websocket.Conn) *wsConn {
	return &wsConn{conn: c}
}

// Send 发送一帧（gorilla 要求单写者，用锁串行化）。
func (w *wsConn) Send(op string, d any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return websocket.ErrCloseSent
	}
	w.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return w.conn.WriteJSON(frame{Op: op, D: d})
}

// CloseWithReason 发送 closed{code,message} 帧后关闭连接（幂等）。
func (w *wsConn) CloseWithReason(code, message string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	w.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_ = w.conn.WriteJSON(frame{Op: "closed", D: map[string]any{"code": code, "message": message}})
	_ = w.conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, code))
	_ = w.conn.Close()
}
