// Package transformer converts ProseMirror/Tiptap JSON and Yjs XML without a
// JavaScript runtime. Applications validate their editor schema through Validate.
package transformer

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf16"

	"github.com/l4iniwakura/HocusPocus-Go/crdt"
)

type Mark struct {
	Type  string         `json:"type"`
	Attrs map[string]any `json:"attrs,omitempty"`
}
type Node struct {
	Type    string         `json:"type"`
	Attrs   map[string]any `json:"attrs,omitempty"`
	Content []Node         `json:"content,omitempty"`
	Marks   []Mark         `json:"marks,omitempty"`
	Text    string         `json:"text,omitempty"`
}
type Prosemirror struct{ Validate func(Node) error }

func (p Prosemirror) FromFields(doc *crdt.Doc, fields ...string) (map[string]Node, error) {
	if len(fields) == 0 {
		fields = doc.RootNames()
	}
	out := map[string]Node{}
	for _, field := range fields {
		node, err := p.FromYDoc(doc, field)
		if err != nil {
			return nil, err
		}
		out[field] = node
	}
	return out, nil
}
func (p Prosemirror) ToYDoc(root Node, fields ...string) (*crdt.Doc, error) {
	if root.Type != "doc" {
		return nil, errors.New("ProseMirror root must be doc")
	}
	if p.Validate != nil {
		if err := p.Validate(root); err != nil {
			return nil, err
		}
	}
	if len(fields) == 0 {
		fields = []string{"prosemirror"}
	}
	for _, n := range root.Content {
		if err := validateShape(n, 0); err != nil {
			return nil, err
		}
	}
	doc := crdt.New()
	for _, field := range fields {
		children := buildChildren(root.Content)
		doc.Transact(func(tx *crdt.Transaction) {
			frag := tx.GetXmlFragment(field)
			for i, child := range children {
				switch n := child.(type) {
				case *crdt.YXmlElement:
					frag.InsertElement(tx, i, n)
				case *crdt.YXmlText:
					frag.InsertText(tx, i, n)
				}
			}
		})
	}
	return doc, nil
}
func validateShape(n Node, depth int) error {
	if depth > 128 || n.Type == "" {
		return errors.New("invalid ProseMirror structure")
	}
	if n.Type == "text" && n.Text == "" {
		return errors.New("empty text node")
	}
	for _, c := range n.Content {
		if err := validateShape(c, depth+1); err != nil {
			return err
		}
	}
	return nil
}
func buildChildren(nodes []Node) []any {
	out := []any{}
	var text *crdt.YXmlText
	offset := 0
	previous := map[string]bool{}
	for _, n := range nodes {
		if n.Type == "text" {
			if text == nil {
				text = crdt.NewYXmlText()
				out = append(out, text)
				offset = 0
				previous = map[string]bool{}
			}
			attrs := crdt.Attributes{}
			for key := range previous {
				attrs[key] = nil
			}
			next := map[string]bool{}
			for _, mark := range n.Marks {
				a := mark.Attrs
				if a == nil {
					a = map[string]any{}
				}
				attrs[mark.Type] = a
				next[mark.Type] = true
			}
			text.Insert(nil, offset, n.Text, attrs)
			previous = next
			offset += len(utf16.Encode([]rune(n.Text)))
			continue
		}
		text = nil
		element := crdt.NewYXmlElement(n.Type)
		for k, v := range n.Attrs {
			element.SetAttributeValue(nil, k, v)
		}
		for i, c := range buildChildren(n.Content) {
			switch child := c.(type) {
			case *crdt.YXmlElement:
				element.InsertElement(nil, i, child)
			case *crdt.YXmlText:
				element.InsertText(nil, i, child)
			}
		}
		out = append(out, element)
	}
	return out
}
func (p Prosemirror) FromYDoc(doc *crdt.Doc, field string) (Node, error) {
	if field == "" {
		field = "prosemirror"
	}
	kind := doc.RootTypeName(field)
	if kind != "xml" && kind != "unknown" && kind != "absent" {
		return Node{}, fmt.Errorf("root %s is %s, expected XML", field, kind)
	}
	root := Node{Type: "doc"}
	for _, child := range doc.GetXmlFragment(field).Children() {
		nodes, err := readNode(child, 0)
		if err != nil {
			return Node{}, err
		}
		root.Content = append(root.Content, nodes...)
	}
	if p.Validate != nil {
		if err := p.Validate(root); err != nil {
			return Node{}, err
		}
	}
	return root, nil
}
func readNode(value any, depth int) ([]Node, error) {
	if depth > 128 {
		return nil, errors.New("ProseMirror nesting limit")
	}
	switch n := value.(type) {
	case *crdt.YXmlElement:
		node := Node{Type: n.NodeName, Attrs: n.GetAttributeValues()}
		for _, child := range n.Children() {
			nodes, err := readNode(child, depth+1)
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, nodes...)
		}
		return []Node{node}, nil
	case *crdt.YXmlText:
		nodes := []Node{}
		for _, delta := range n.ToDelta() {
			text, ok := delta.Insert.(string)
			if !ok {
				return nil, errors.New("unsupported embedded ProseMirror text value")
			}
			if text == "" {
				continue
			}
			node := Node{Type: "text", Text: text}
			keys := make([]string, 0, len(delta.Attributes))
			for key := range delta.Attributes {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				value := delta.Attributes[key]
				if value == nil {
					continue
				}
				attrs, ok := value.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("mark %s is not an object", key)
				}
				kind, _, _ := strings.Cut(key, "--")
				node.Marks = append(node.Marks, Mark{Type: kind, Attrs: attrs})
			}
			nodes = append(nodes, node)
		}
		return nodes, nil
	default:
		return nil, fmt.Errorf("unsupported XML node %T", value)
	}
}
