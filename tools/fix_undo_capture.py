"""Make a zero UndoManager capture window disable grouping on all clocks."""
from pathlib import Path

path = Path(__file__).resolve().parents[1] / 'crdt/undo.go'
source = path.read_text(encoding='utf-8')
needle = 'now.Sub(u.lastTxnTime) <= u.captureTimeout'
assert source.count(needle) == 1
path.write_text(source.replace(needle, 'u.captureTimeout > 0 && now.Sub(u.lastTxnTime) < u.captureTimeout'), encoding='utf-8')
