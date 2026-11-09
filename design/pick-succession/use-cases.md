# Use cases

| | intention | How |
|---|---|---|
| **Problem** | A write under a pick does not always replace what it meant to, and a claim that was replaced can win a read. | |
| 1 | A writer writes a value under a pick and reads it back under the same pick. | |
| 2 | Two writers wrote a cell without seeing each other; a later writer who sees both settles it to one value. | |
| 3 | A writer appends values under `all`, and they stay, whatever others write under an electing pick. | |
| 4 | A replica that holds only part of a tree reads every cell it holds correctly. | |
| 5 | Two replicas that hold the same claims read the same value. | |
