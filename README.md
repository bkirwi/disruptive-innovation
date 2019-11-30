# Notes

Current difficulty:
I'd like to support the pattern where each entry of buffer A
creates a corresponding entry in buffer B,
and some handler can read from both A and B.
We need to create a SequenceBarrier
that depends on both A and B,
but the stock EventProcessor only takes one barrier as input.

Maybe it's safe to just use the original input?

So: seems like it ought to? Given that:

- For whatever published to A, we know that its sequence will only increment
  when the corresponding message is safely published. Ditto B.
  
We'll need to make sure to thread the deps correctly.
eg. A and B will both need to gate on the final handler.

=========

Seems I want both monad and applicative here.
In particular we seem to need to do some static analysis
to know how to set up all the dependencies correctly.