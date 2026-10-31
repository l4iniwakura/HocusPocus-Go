"""Apply the two Hocuspocus API additions to unmodified ygo v1.54.0 sources."""
from pathlib import Path

root = Path(__file__).resolve().parents[1] / 'crdt'
doc = root / 'doc.go'
source = doc.read_text(encoding='utf-8')
needle = 'gc              bool'
assert source.count(needle) == 1, 'Expected the pinned upstream Doc layout'
doc.write_text(source.replace(needle, needle + '\n\tgcFilter func(*Item) bool'), encoding='utf-8')
gc = root / 'snapshot.go'
source = gc.read_text(encoding='utf-8')
needle = 'item.Content = NewContentDeleted(item.Content.Len())'
assert source.count(needle) == 2, 'Expected both pinned upstream GC paths'
gc.write_text(source.replace(needle, 'if doc.gcFilter == nil || doc.gcFilter(item) { ' + needle + ' }'), encoding='utf-8')
