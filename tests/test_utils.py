from jlr.utils import by_chunks



def test_by_chunks():

    def chunks_as_list(chunked_iter):
        return list(chunked_iter)

    assert chunks_as_list(by_chunks([], 3)) == [], chunks_as_list(by_chunks([], 3))

    assert chunks_as_list(by_chunks([1,2,], 3)) == [[1,2]]

    assert chunks_as_list(by_chunks([1,2,3,], 3)) == [[1,2,3]]

    assert chunks_as_list(by_chunks([1,2,3,4], 3)) == [[1,2,3], [4]]

    assert chunks_as_list(by_chunks([1,2,3,4,5,6], 3)) == [[1,2,3], [4,5,6]]

    assert chunks_as_list(by_chunks([1,2,3,4,5,6,7], 3)) == [[1,2,3], [4,5,6], [7,]]

    # Now test padding by working down ...
    assert chunks_as_list(by_chunks([1,2,3,4,5,6,7], 3, pad_with=None)) == [[1,2,3], [4,5,6], [7,None,None]]

    assert chunks_as_list(by_chunks([1,2,3,4,5,6], 3, pad_with=None)) == [[1,2,3], [4,5,6]]

    assert chunks_as_list(by_chunks([1,2,3,4,5], 3, pad_with=None)) == [[1,2,3], [4,5,None]]

    assert chunks_as_list(by_chunks([1,], 3, pad_with=None)) == [[1,None, None]]

    assert chunks_as_list(by_chunks([], 3, pad_with=None)) == []

    # Things other than None can be used.
    assert chunks_as_list(by_chunks([1,2,3,4,5], 3, pad_with='sdf')) == [[1,2,3], [4,5,'sdf']]


