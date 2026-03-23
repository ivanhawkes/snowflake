# Snowflake - A GoLang Implementation of a 64 bit distributed ID Generator

## Introduction

This project is based upon a concept and work created by Twitter. They needed to solve the
issue of primary key conflicts when inserting rows of data into tables from potentially
hundreds of servers simultaneously. The IDs need to be unique, without the overhead of
network communications, thread locks, or other typical solutions.

At the same time, it is desireable if the generated numbers don't thrash B+ Tree
structures when inserting large amounts quickly. Ensuring the numbers are in a
semi-sorted order can alleviate a lot of that issue.

The solution comes with some compromises, but the speed and efficiency gains
are worth the few shortcomings it has.

## The Algorithm

This code is based on that concept; and several code bases I've found online. I'm copying
the best parts of each, and hacking out the extras they all want to slap on. The key
generator should be completely agnostic of it's operating environment.

First, take a date offset and express it as an integral multiple of a time period
from a fixed point known as the "epoch". This is exactly how the Unix clock works
so Twitter simply borrowed an old idea and upgraded the number of bits available
to the original (though less than the current implementation).

Twitter selected 1ms as the time period to bucket new keys into. On server machines
it can be hard to get a reliable clock source that has a higher frequency than 1ms
so this is a good starting point.

## TODO

* remove sleep on fail, make it just pass back the exhaustion flag
* write a strategy
* write a high speed test
