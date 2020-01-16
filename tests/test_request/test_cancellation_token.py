from lazyflow.request.cancellation_token import CancellationTokenSource, CancellationToken, UNCANCELLABLE_TOKEN, NULL_CANCELLATION_TOKEN


def test_combining_cancel_token_with_uncancellable():
    token = UNCANCELLABLE_TOKEN
    token |= CancellationToken()
    assert token is UNCANCELLABLE_TOKEN

    token2 = CancellationToken()
    token2 |= UNCANCELLABLE_TOKEN
    assert token2 is UNCANCELLABLE_TOKEN


def test_combining_cancel_token_with_null():
    token = NULL_CANCELLATION_TOKEN
    rhs = CancellationToken()
    token |= rhs
    assert token is rhs

    orig_token = CancellationToken()
    token = orig_token
    rhs = NULL_CANCELLATION_TOKEN
    token |= rhs
    assert token is orig_token


def test_combining_cancel_token_with_other_token():
    src1 = CancellationTokenSource()
    src2 = CancellationTokenSource()

    token1 = src1.token
    token2 = src2.token

    combined = token1 | token2

    src1.cancel()
    assert token1.cancelled
    assert not token2.cancelled
    assert not combined.cancelled

    src2.cancel()
    assert combined.cancelled
