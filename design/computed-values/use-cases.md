# A value computed from many writers' claims either loses writes or keeps every one: use cases

Problem: an application wants a value many writers change at once and
read as one, but concurrent writes to one claim keep one of them, keeping
each write grows the store with the writes, and an authored rule cannot
compute the value.

| intention | How |
|---|---|
| Count clicks made on several devices, offline and then synced, and read the total that every write contributed | |
| Count increments made by many agents on separate branches of one space, merged into main | |
| Show a total that falls as well as rises: votes for and against, stock in and out | |
| Show an average or a distribution of readings many devices report | |
| Keep one value per device, read on that device alone, without other devices' values competing with it | |
| Read on one device a combination of every device's own value: the latest reading of each, the furthest each has got | |
| Keep a count's storage from growing with how many times it was incremented | |
| Read a computed value in a rule, and decide on it | |
| Know, as an application author, which values can be changed concurrently without loss and which need coordination | |
