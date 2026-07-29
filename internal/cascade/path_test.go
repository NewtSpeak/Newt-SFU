package cascade

import (
	"testing"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
)

func TestClassifyPath(t *testing.T) {
	cases := []struct {
		local, remote string
		want          owlsfuv1.EdgeStatus_PathType
	}{
		{"10.0.0.1", "10.0.0.2", owlsfuv1.EdgeStatus_PATH_TYPE_LAN},
		{"192.168.1.1", "172.16.0.5", owlsfuv1.EdgeStatus_PATH_TYPE_LAN},
		{"10.0.0.1", "203.0.113.10", owlsfuv1.EdgeStatus_PATH_TYPE_WAN},
		{"203.0.113.1", "198.51.100.2", owlsfuv1.EdgeStatus_PATH_TYPE_WAN},
		{"", "10.1.2.3", owlsfuv1.EdgeStatus_PATH_TYPE_LAN},
		{"", "8.8.8.8", owlsfuv1.EdgeStatus_PATH_TYPE_WAN},
		{"", "", owlsfuv1.EdgeStatus_PATH_TYPE_UNSPECIFIED},
		{"not-an-ip", "also-bad", owlsfuv1.EdgeStatus_PATH_TYPE_UNSPECIFIED},
	}
	for _, tc := range cases {
		got := classifyPath(tc.local, tc.remote)
		if got != tc.want {
			t.Errorf("classifyPath(%q,%q)=%v want %v", tc.local, tc.remote, got, tc.want)
		}
	}
}

func TestIsPrivateIPString(t *testing.T) {
	priv, ok := isPrivateIPString("10.0.0.1")
	if !ok || !priv {
		t.Fatal("10.0.0.1 should be private")
	}
	priv, ok = isPrivateIPString("1.1.1.1")
	if !ok || priv {
		t.Fatal("1.1.1.1 should be public")
	}
	_, ok = isPrivateIPString("")
	if ok {
		t.Fatal("empty should not parse")
	}
}
