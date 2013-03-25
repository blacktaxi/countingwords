This is my solution to the programming problem of the Berlin fun club's 2013 february meetup. See http://www.meetup.com/thefunclub/events/104441382/ .

There are actually two solutions -- a blunt sequential read/parse/count, and a more parallel solution, which parses the text in parallel and then merges calculated frequency tables.

On my workstation, which is a Core i5-750 box, the run times (very rough) are as follows:

    moby-dic.txt, ~1.2mb:
        Sequential: 0.18s, ~9Mb
        Parallel: 0.14s, ~20Mb

    moby-dic1.txt, ~25mb:
        Sequential: 3.1s, ~9Mb
        Parallel: 2.1s, ~200Mb

    moby-dic2.txt, ~50mb:
        Sequential: 6.3s, ~9Mb
        Parallel: 3.6s, ~300Mb

    moby-dic3.txt, ~500mb:
        Sequential: 6.3s, ~9Mb
        Parallel: 36s, ~1100Mb

There's an issue with parallel version that given bad luck, input file can be fragmented in such way that there will be a part (usually the last one) that wholly consists of a single word. I didn't handle that case, so there's an assertion that will fail. I'll try to fix it up if I come back later to this.




    