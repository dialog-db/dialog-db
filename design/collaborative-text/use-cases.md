# Use cases: collaborative text editing

Problem: several people edit the same text, peer to peer and offline,
and every replica converges without lost edits. Last writer wins on a
text field loses one side.

| intention | needed | How |
|---|---|---|
| two people edit the same plain text concurrently, online or offline, no server, and both edits survive | required | |
| text the size of a paragraph or a code cell | required | |
| storage and sync cost within 2× of a text CRDT library on the same edit history | required | |
| sync knows which documents need syncing without asking about each one, as asset facts do for assets | required | |
| text the size of a book chapter | nice to have | |
| rich text (bold, links) | nice to have | |
| concurrent runs typed at one spot do not interleave | nice to have | |
| a query returns a document's current text as a value | nice to have | |
| search or join inside the text (find a note, link a paragraph) | nice to have | |
| bring whichever text CRDT library you use | nice to have | |
