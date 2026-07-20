package cascade

// 级联信令线协议（15 BG.2）：child 主动连 parent 级联端口（mTLS），
// 连接建立后互发 JSON 帧（json.Encoder/Decoder 流式编解码，无需额外分帧）。
//
// 每条边一条 TCP 连接，其上承载两个方向的 PC 协商：
//   - dir="up"  ：child → parent 的媒体 PC（child 为 offerer）
//   - dir="down"：parent → child 的媒体 PC（parent 为 offerer）
//
// 单方向单 offerer，天然规避双向 renegotiation glare。

const (
	dirUp   = "up"
	dirDown = "down"
)

// frame 为级联信令帧（t 区分类型，其余字段按需填充）。
type frame struct {
	T string `json:"t"`

	// hello（child → parent 首帧）
	RoomID string `json:"room_id,omitempty"`
	Epoch  uint64 `json:"epoch,omitempty"`
	Child  string `json:"child,omitempty"`
	Token  string `json:"token,omitempty"`

	// hello_ack
	OK     bool   `json:"ok,omitempty"`
	Reason string `json:"reason,omitempty"`

	// offer / answer / ice
	Dir       string  `json:"dir,omitempty"`
	SDP       string  `json:"sdp,omitempty"`
	Candidate string  `json:"candidate,omitempty"`
	SDPMid    *string `json:"sdp_mid,omitempty"`
	SDPMLine  *uint16 `json:"sdp_mline,omitempty"`

	// want（NodeWant 通报，双向均可发；语义 = 「我方需要你送来的 speaker 集合」）。
	// Want 为音频需求；ScreenWant 为屏幕轨（含系统音频伴轨）需求，两者独立剪枝：
	// 观看端按需订阅，某 speaker 的屏幕在本枝无人订阅时不得跨节点拉流（08 D.4/5.2）。
	// 对端未携带 ScreenWant（旧版本节点）视为无屏幕需求 → 不送屏幕轨（安全缺省）。
	// （PLI/FIR 关键帧请求不走信令帧：直接经媒体 PC 的 RTCP 通道逐跳回传，见 edge.go）
	Want       *wireWant `json:"want,omitempty"`
	ScreenWant *wireWant `json:"screen_want,omitempty"`

	// ping / pong（RTT 探测；pong 回显 ts）
	TS int64 `json:"ts,omitempty"`
}

// 帧类型。
const (
	frameHello    = "hello"
	frameHelloAck = "hello_ack"
	frameOffer    = "offer"
	frameAnswer   = "answer"
	frameICE      = "ice"
	frameWant     = "want"
	framePing     = "ping"
	framePong     = "pong"
)
