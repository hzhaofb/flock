# How to performance test flock

1. source gen-data.sql to generate large amount of tasks (2 billion)

> call genSeq(1000000);

> call popData(2000);

2. run flock servers in 2+ machines
3. configure HAProxy to front these flock servers
4. start [app/flocker](../../flocker) on some worker machine. 
   You probably want to start many flocker processes in a machine since 
   they only stresses flock servers.
    