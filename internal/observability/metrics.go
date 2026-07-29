// Package observability 提供 Prometheus 指标与 /metrics + pprof HTTP 服务。
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics 汇总 newt-sfu 全部 Prometheus 指标。
type Metrics struct {
	Registry *prometheus.Registry

	Participants            prometheus.Gauge
	Rooms                   prometheus.Gauge
	ScreenTracks            prometheus.Gauge
	RTPForwardedPackets     prometheus.Counter
	RTPForwardedBytes       prometheus.Counter
	SignalConnects          prometheus.Counter
	SignalAuthFailures      *prometheus.CounterVec
	JoinDuration            prometheus.Histogram
	DisconnectCmdLatency    prometheus.Histogram
	ControlCommands         *prometheus.CounterVec
	CommandLatency          *prometheus.HistogramVec
	ControlReconnects       prometheus.Counter
	SpeakingEventsPublished prometheus.Counter

	// ---- 级联（M3，docs 08 §11）----
	CascadeEdges           prometheus.Gauge       // 当前活跃级联边数
	CascadeEdgeUp          prometheus.Counter     // EdgeUp 累计
	CascadeEdgeDown        prometheus.Counter     // EdgeDown 累计
	CascadeEdgeRTT         *prometheus.GaugeVec   // 每边 RTT ms（room, peer_node）
	CascadeEdgeBytes       *prometheus.CounterVec // 每边收/发字节（room, peer_node, dir）
	CascadeEdgePackets     *prometheus.CounterVec // 每边收/发包数（room, peer_node, dir）
	CascadeOutboundTracks  *prometheus.GaugeVec   // 出向轨数（state=active|pruned）：剪枝率 = pruned/(active+pruned)
	CascadeLoopDropped     prometheus.Counter     // 环路/非预期来源丢弃（08 C.5）
	CascadeLeaseExpired    prometheus.Counter     // 租约过期导致停转发次数
	CascadeHandshakeFailed *prometheus.CounterVec // 级联信令握手失败（reason）

	// ---- 热迁移（M4，docs 09 §11）----
	MigratedSessions *prometheus.CounterVec // 迁出会话数（result=ok|not_found）
}

// NewMetrics 构建并注册全部指标（含 Go runtime/process 默认采集器）。
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		Participants: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "owlsfu_participants", Help: "Current media participants.",
		}),
		Rooms: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "owlsfu_rooms", Help: "Current local rooms.",
		}),
		ScreenTracks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "owlsfu_screen_tracks", Help: "Current active screen-share video tracks.",
		}),
		RTPForwardedPackets: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_rtp_forwarded_packets_total", Help: "Total RTP packets forwarded downstream.",
		}),
		RTPForwardedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_rtp_forwarded_bytes_total", Help: "Total RTP bytes forwarded downstream.",
		}),
		SignalConnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_signal_connects_total", Help: "Total websocket signaling connections accepted.",
		}),
		SignalAuthFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "owlsfu_signal_auth_failures_total", Help: "Total signaling auth failures by close code.",
		}, []string{"code"}),
		JoinDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "owlsfu_join_duration_seconds",
			Help:    "Latency from auth accepted to media (PeerConnection) connected.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		DisconnectCmdLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "owlsfu_disconnect_command_latency_seconds",
			Help:    "Latency from DisconnectUser/RevokeSession command receipt to session teardown.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}),
		ControlCommands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "owlsfu_control_commands_total", Help: "Total control commands processed by type and result.",
		}, []string{"type", "result"}),
		CommandLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "owlsfu_control_command_latency_seconds",
			Help:    "Control command handling latency by type.",
			Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"type"}),
		ControlReconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_control_reconnects_total", Help: "Total control channel reconnect attempts.",
		}),
		SpeakingEventsPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_speaking_events_total", Help: "Total speaking events pushed to clients.",
		}),
		CascadeEdges: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "owlsfu_cascade_edges", Help: "Current established cascade edges.",
		}),
		CascadeEdgeUp: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_cascade_edge_up_total", Help: "Total cascade EdgeUp transitions.",
		}),
		CascadeEdgeDown: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_cascade_edge_down_total", Help: "Total cascade EdgeDown transitions.",
		}),
		CascadeEdgeRTT: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "owlsfu_cascade_edge_rtt_ms", Help: "Cascade edge signaling RTT in milliseconds.",
		}, []string{"room", "peer_node"}),
		CascadeEdgeBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "owlsfu_cascade_edge_bytes_total", Help: "Total RTP bytes over cascade edges by direction.",
		}, []string{"room", "peer_node", "dir"}),
		CascadeEdgePackets: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "owlsfu_cascade_edge_packets_total", Help: "Total RTP packets over cascade edges by direction.",
		}, []string{"room", "peer_node", "dir"}),
		CascadeOutboundTracks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "owlsfu_cascade_outbound_tracks",
			Help: "Cascade outbound speaker tracks by state (pruned = suppressed by NodeWant).",
		}, []string{"state"}),
		CascadeLoopDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_cascade_loop_dropped_total",
			Help: "Total packets/tracks dropped due to loop defense (unexpected origin, docs 08 C.5).",
		}),
		CascadeLeaseExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "owlsfu_cascade_lease_expired_total",
			Help: "Times cross-node forwarding was halted due to anchor lease expiry.",
		}),
		CascadeHandshakeFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "owlsfu_cascade_handshake_failed_total",
			Help: "Cascade signaling handshake failures by reason.",
		}, []string{"reason"}),
		MigratedSessions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "owlsfu_migrated_sessions_total",
			Help: "Sessions removed by MigrateParticipants by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.Participants, m.Rooms, m.ScreenTracks,
		m.RTPForwardedPackets, m.RTPForwardedBytes,
		m.SignalConnects, m.SignalAuthFailures, m.JoinDuration,
		m.DisconnectCmdLatency, m.ControlCommands, m.CommandLatency, m.ControlReconnects,
		m.SpeakingEventsPublished,
		m.CascadeEdges, m.CascadeEdgeUp, m.CascadeEdgeDown, m.CascadeEdgeRTT,
		m.CascadeEdgeBytes, m.CascadeEdgePackets, m.CascadeOutboundTracks,
		m.CascadeLoopDropped, m.CascadeLeaseExpired, m.CascadeHandshakeFailed,
		m.MigratedSessions,
	)
	return m
}
