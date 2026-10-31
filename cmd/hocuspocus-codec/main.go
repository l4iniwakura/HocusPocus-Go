// Command hocuspocus-codec converts ProseMirror JSON and Yjs V1 updates on stdin.
package main

import (
	"encoding/json"
	"fmt"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
	"github.com/l4iniwakura/HocusPocus-Go/transformer"
	"io"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	var input struct {
		Binary   []byte            `json:"binary"`
		Document *transformer.Node `json:"document"`
		Field    string            `json:"field"`
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 64<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return err
	}
	field := input.Field
	if field == "" {
		field = "prosemirror"
	}
	codec := transformer.Prosemirror{}
	var doc *crdt.Doc
	var err error
	if input.Document != nil {
		doc, err = codec.ToYDoc(*input.Document, field)
	} else {
		doc = crdt.New()
		err = doc.ApplyUpdate(input.Binary)
	}
	if err != nil {
		return err
	}
	node, err := codec.FromYDoc(doc, field)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Binary   []byte           `json:"binary"`
		Document transformer.Node `json:"document"`
	}{doc.EncodeStateAsUpdate(), node})
}
