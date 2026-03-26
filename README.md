# Snowflake

This is my implementation of a Snowflake ID generator. I've tuned it for high
performance, adjusting the methodology found in many popular generators. I've
tried to adjust it for the expected performance characteristics of modern
hardware and high TPS systems.

It's parameters can be tuned for specific workloads and a wide variety of
situations.

I've removed the concept of machines / workers and made it manage just
thread IDs instead. I've tightened up the bit usage quite a bit, trading
off some longevity for higher throughput under even highly parallel loads.

It is perfectly capable of creating millions of IDs per second if needed.

## The Algorithm

The original Twitter algorithm used a 64 bit unsigned integer. By breaking
it down into sections, each with it's own allocation of bits, they vastly
reduced the risk of two IDs having the same value.

They took a leaf out of Unix's book and used 41 bits and a date / time offset
to provide a timestamp. By putting these bits in the most significant position
of the identifier they ensured good sorting in general when the IDs are
placed into B+ Trees (database indexes).

Next came 10 bits for the machine ID and worker ID, five bits each. This may
have made sense in 2010 but it doesn't any more. That would limit the
algorithm to handling just 32 machines, with 32 workers each. Today's
servers can have hundreds of cores and there may be hundreds of
servers deployed.

Finally, an incrementing unsigned integer takes the remaining 12 bits of room.
The topmost bit is wasted, most likely because the SQL Language
doesn't have a definition for unsigned integers.

## Issues

There are a couple of issues with the original design.

1.  There are not enough bits dedicated to machine and worker IDs.
2.  There are too many bits for the timestamp. If a program is still
    running 79 years from now it deserves an upgrade to 128 bit
    identifiers.
3.  There are too many bits in the sequence ID for typical operation.

I've re-allocated all the bits. Switching to just 37 bits for the
timestamp, but making each time period 10ms still gives us 43 years
of operational time. By then the software will either be replaced
or can be upgraded to 128 bit identifiers.

I've reduced the sequence to just 7 bits, giving it a range of 128
values. You can generate 128 IDs in 10ms, or 12,800 / second on a
single thread assuming an even distribution. That's faster than
VISA (1,700 TPS), and even Twitter (6,000 TPS) at typical volumes.

This leaves 19 bits for the ThreadID, a combination of the machine ID
and worker ID. That's nine more bits, and every single one of them
independantly able to be used; 512 times the address space of the
original algorithm.

## The Concept

Wring every last drop of blood out of every last bit.

While the end user is free to set their own epoch, I've moved the
default epoch to March 2026, saving 16 years of addressable dates
from being wasted.

The sequence ID could be made even smaller than seven bits, but this
seems a reasonable compromise for times when there is high burst
traffic e.g. launch day on a new service.

With 19 bits of address space (524,288) for threads you can allocate
a threadID to every single core of every machine on any workload
I currently anticipate.

## The New Algorithm

I've dropped the number of bits for the sequence down to seven, which
seems like I may have hamstrung the system. In exchange we are able
to use those bits more freely at a higher level of abstraction.

I've added a "strategy" as a layer above the Snowflake.

If you run Snowflake at full tilt, with zero time spent processing
each new record, then it's pretty easy to run out of sequence IDs.
That would choke the system. You are not meant to run the bare
Snowflake code any more, instead you use a strategy to manage those
Snowflakes.

You create a pool of strategies which you can then use for ID allocation.
You can create as large or small a pool as you wish, but it is
recommended you make a pool with one strategy for every core your
server has available. Each strategy manages one Snowflake generator.

When you request a new ID from your strategy it will grab one from
it's Snowflake. The Snowflake will let it know if it's been running
too hot and has run out of sequence ID space. The strategy will then
consult a pool of reserve Thead IDs, putting one of those into the
Snowflake so it can keep generating.

It will do that until it has exhausted all of it's reserves, or the
timestamp ticks over, which happens every 10ms. When there is a new
timestamp it returns all the exhuasted Thread IDs to the reserve pool and
sorts it into order (for re-use reasons).

You can tune the performance however you like. As long as you have
either enough vertical scale out or reserve thread pools it will
run just fine. There is a situation where you are simply requesting
far more IDs in a shorter timespan than the system is designed to take.

That shouldn't happen though, since it's almost certain the generators
will be sitting idle while waiting for records to write to the disks.

Secondly, you shouldn't allow an unlimited amount of write attempts on
your websites. Put some limiters on the front end or in the middleware.

For those two reasons I have chosen to simply log the failure conditions
should they occur. It will attempt to keep running regardless.

## Tune for Performance

With all that said, what settings should you use?

I suggest you create one strategy per core on each machine. You will
have to decided how many thread IDs you keep in reserve for each core.

Try running the test program a few times with different parameters and
seeing if it works or you've pushed it hard enough to create an error
condition.

You will need to play with the sleep() function to simulate time lost
to processing the records.

Honestly, most sites wouldn't even come close to needing the amount of
IDs this can generate on a single thread with no reserves.

e.g. my personal work machine - a 24 core 4ghz AMD.

I have set it to sleep for 1 microsecond, an incredibly small amount of time
for even the fastest machines. Tests generating 8,192 IDs per strategy with
24 of those in a pool give these results:

196,608 IDs created in 147.38ms

They used only 3 of the 16 reserve thread IDs I provided to them.

There's plenty of room to scale vertically and have larger reserves if you
expect to have millions of records per second needing a unique ID. In theory
it would top out at 6,710,886,400 / sec if it was perfectly balanced.
