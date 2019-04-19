from itertools import tee, chain

no_padding=object()
def by_chunks(iterable, chunk_size, pad_with=no_padding):
    iterator = iter(iterable)
    empty = False
    while not empty:
        chunk = []
        for _ in range(0, chunk_size):
            try:
                chunk.append(next(iterator)) # StopIteration from iterator breaks loop
            except StopIteration:
                empty = True

        # Only produce if has any members.
        if chunk:
            if pad_with is not no_padding: # Ah, Southern English
                chunk.extend([pad_with] * (chunk_size - len(chunk)))

            yield chunk


def prior_current_next(iterable, pad=None):
    """"
        s -> (pad, s0, s1), (s0,s1,s2),  ..., (sN-1, sN, pad).

        Aka 'prior, current, and next' elements in an iteration all
        at once.

        Lets you visit each element in an iterable while also being
        presented with the prior and next element. At the beginning and
        end of the iteration the prior and/or next element will
        be presented as the 'pad' value.

        Similar to using SQL windowing functions LAG and LEAD
        at the same time.
    """

    # Get three iterables out of the one.
    a, b, c = tee(iterable, 3)

    # Hook up to drain the pad value once from the first iterable as
    # the initial degenerate 'prior' value.
    a = chain([pad], a)

    # Likewise mirror image for the 'next' value.
    c = chain(c, [pad])

    # Must pre-consume from next before returning.
    next(c, None)

    # Return zipped wrapping of all three.
    return zip(a, b, c)
