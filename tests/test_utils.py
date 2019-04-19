from jlr.utils import by_chunks, prior_current_next



def test_by_chunks():

    assert list(by_chunks([], 3)) == [], list(by_chunks([], 3))

    assert list(by_chunks([1,2,], 3)) == [[1,2]]

    assert list(by_chunks([1,2,3,], 3)) == [[1,2,3]]

    assert list(by_chunks([1,2,3,4], 3)) == [[1,2,3], [4]]

    assert list(by_chunks([1,2,3,4,5,6], 3)) == [[1,2,3], [4,5,6]]

    assert list(by_chunks([1,2,3,4,5,6,7], 3)) == [[1,2,3], [4,5,6], [7,]]

    # Now test padding by working down ...
    assert list(by_chunks([1,2,3,4,5,6,7], 3, pad_with=None)) == [[1,2,3], [4,5,6], [7,None,None]]

    assert list(by_chunks([1,2,3,4,5,6], 3, pad_with=None)) == [[1,2,3], [4,5,6]]

    assert list(by_chunks([1,2,3,4,5], 3, pad_with=None)) == [[1,2,3], [4,5,None]]

    assert list(by_chunks([1,], 3, pad_with=None)) == [[1,None, None]]

    assert list(by_chunks([], 3, pad_with=None)) == []

    # Things other than None can be used.
    assert list(by_chunks([1,2,3,4,5], 3, pad_with='sdf')) == [[1,2,3], [4,5,'sdf']]

def test_prior_current_next():

    # Basic usage.
    assert list(prior_current_next([1,2,3,4])) == [(None, 1, 2), (1,2,3), (2,3,4), (3,4,None)]

    # Overriding the pad value...
    assert list(prior_current_next([1,2,3,4], -1)) == [(-1, 1, 2), (1,2,3), (2,3,4), (3,4,-1)]

    # Short input
    assert list(prior_current_next([1,])) == [(None, 1, None)]

    # Really short input
    assert list(prior_current_next([])) == []

