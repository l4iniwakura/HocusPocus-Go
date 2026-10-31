package transformer

import (
	"encoding/json"
	"testing"
)

func TestRichDocumentRoundtrip(t *testing.T) {
	root := Node{Type: "doc", Content: []Node{{Type: "paragraph", Attrs: map[string]any{"align": "center"}, Content: []Node{{Type: "text", Text: "Hello 👋", Marks: []Mark{{Type: "bold"}, {Type: "link", Attrs: map[string]any{"href": "https://example.org"}}}}, {Type: "text", Text: " plain"}}}, {Type: "table", Content: []Node{{Type: "tableRow", Content: []Node{{Type: "tableCell", Attrs: map[string]any{"colspan": float64(2)}, Content: []Node{{Type: "paragraph"}}}}}}}}}
	p := Prosemirror{}
	doc, err := p.ToYDoc(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.FromYDoc(doc, "")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(root)
	b, _ := json.Marshal(result)
	if string(a) != string(b) {
		t.Fatalf("%s != %s", a, b)
	}
}
func TestRejectInvalidRoot(t *testing.T) {
	if _, err := (Prosemirror{}).ToYDoc(Node{Type: "paragraph"}); err == nil {
		t.Fatal("accepted non-document root")
	}
}
func TestCustomSchemaValidation(t *testing.T) {
	called := false
	p := Prosemirror{Validate: func(Node) error { called = true; return nil }}
	_, err := p.ToYDoc(Node{Type: "doc"})
	if err != nil || !called {
		t.Fatal(err, called)
	}
}
