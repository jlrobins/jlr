
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


