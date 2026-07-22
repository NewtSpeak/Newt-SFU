package cascade

import (
	"net"

	"github.com/pion/webrtc/v4"

	owlsfuv1 "github.com/owlspeak/owl-sfu/gen/owlsfu/v1"
)

// selectedPairIPs 从 PC 的 ICE selected candidate pair 取本端/对端 IP。
// 优先走 Sender/Receiver → DTLS → ICE；任一路径可用即返回。
func selectedPairIPs(pc *webrtc.PeerConnection) (localIP, remoteIP string, ok bool) {
	if pc == nil {
		return "", "", false
	}
	// 发送侧
	for _, sender := range pc.GetSenders() {
		if sender == nil {
			continue
		}
		if dtls := sender.Transport(); dtls != nil {
			if ice := dtls.ICETransport(); ice != nil {
				if pair, err := ice.GetSelectedCandidatePair(); err == nil && pair != nil {
					return candidateIP(pair.Local), candidateIP(pair.Remote), true
				}
			}
		}
	}
	// 接收侧
	for _, recv := range pc.GetReceivers() {
		if recv == nil {
			continue
		}
		if dtls := recv.Transport(); dtls != nil {
			if ice := dtls.ICETransport(); ice != nil {
				if pair, err := ice.GetSelectedCandidatePair(); err == nil && pair != nil {
					return candidateIP(pair.Local), candidateIP(pair.Remote), true
				}
			}
		}
	}
	return "", "", false
}

func candidateIP(c *webrtc.ICECandidate) string {
	if c == nil {
		return ""
	}
	return c.Address
}

// classifyPath 根据候选 IP 是否私网判断内网/外网。
// 规则：两端均为私网（或一端为空另一端私网）→ LAN；任一端为公网 → WAN；无法判断 → UNSPECIFIED。
func classifyPath(localIP, remoteIP string) owlsfuv1.EdgeStatus_PathType {
	localPriv, localKnown := isPrivateIPString(localIP)
	remotePriv, remoteKnown := isPrivateIPString(remoteIP)
	switch {
	case localKnown && remoteKnown:
		if localPriv && remotePriv {
			return owlsfuv1.EdgeStatus_PATH_TYPE_LAN
		}
		return owlsfuv1.EdgeStatus_PATH_TYPE_WAN
	case remoteKnown:
		if remotePriv {
			return owlsfuv1.EdgeStatus_PATH_TYPE_LAN
		}
		return owlsfuv1.EdgeStatus_PATH_TYPE_WAN
	case localKnown:
		if localPriv {
			return owlsfuv1.EdgeStatus_PATH_TYPE_LAN
		}
		return owlsfuv1.EdgeStatus_PATH_TYPE_WAN
	default:
		return owlsfuv1.EdgeStatus_PATH_TYPE_UNSPECIFIED
	}
}

// isPrivateIPString 解析 IP 并判断是否 RFC1918/链路本地/回环等非公网地址。
// 第二返回值表示解析是否成功。
func isPrivateIPString(s string) (private bool, ok bool) {
	if s == "" {
		return false, false
	}
	ip := net.ParseIP(s)
	if ip == nil {
		// host:port 或带 zone 的形式兜底
		if host, _, err := net.SplitHostPort(s); err == nil {
			ip = net.ParseIP(host)
		}
	}
	if ip == nil {
		return false, false
	}
	return isPrivateIP(ip), true
}

func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() {
		return true
	}
	// 额外：Carrier-grade NAT 100.64.0.0/10、IPv6 ULA fc00::/7 已由 IsPrivate 覆盖（Go 1.17+）
	return false
}
