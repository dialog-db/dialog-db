# A replica cannot read or write what it held once a pull moves its head: use cases

Problem: a partial replica loses the ability to read and write, from its
own blocks, each key whose leaf another replica changed, every time it
adopts that replica's head by root.

| intention | How |
|---|---|
| Show again, with no network, a page the device showed before, whatever its peers wrote since | |
| Answer an edit to something on screen at once, whatever its peers wrote since | |
| Keep reading and writing while a sync is under way, on a slow link as on a fast one | |
| Take a peer's changes without paying for the parts of the space this device never looked at | |
| Know, as an embedder, which reads and writes can wait on the network and which cannot | |
| Bring a replica that is already standing on a head it holds little of back to working offline | |
