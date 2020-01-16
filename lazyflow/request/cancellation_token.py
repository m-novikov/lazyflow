class ICancellationToken:
    @property
    def cancelled(self):
        raise NotImplementedError


class CancellationTokenSource:
    __slots__ = ("token",)

    def __init__(self):
        self.token = CancellationToken()

    def cancel(self):
        self.token._cancelled = True


class CancellationToken(ICancellationToken):
    __slots__ = ("_cancelled",)

    def __init__(self):
        self._cancelled = False

    @property
    def cancelled(self):
        return self._cancelled

    def __repr__(self):
        return f"CancellationToken(id={id(self)}, cancelled={self._cancelled})"


class CombinedCancellationToken(ICancellationToken):
    def __init__(self, *tokens):
        self.__tokens = tokens

    @property
    def cancelled(self) -> bool:
        return bool(self.__tokens) and all(t.cancelled for t in self.__tokens)

    def add_token(self, token):
        self.__tokens.append(token)
        return self

    def __or__(self, other: ICancellationToken):
        if isinstance(other, CancellationToken):
            return _CombinedCancellationToken(self, other)
        elif isinstance(other, _CombinedCancellationToken):
            return other.add_token(self)
        return NotImplemented

    def __ior__(self, value: ICancellationToken):
        return self.__or__(value)


class _NullCancellationToken(ICancellationToken):
    @property
    def cancelled(self):
        return False

    def __or__(self, other: ICancellationToken):
        return other

    def __ror__(self, other: ICancellationToken):
        return self.__or__(other)

    def __ior__(self, value: ICancellationToken):
        return self.__or__(value)


class _UncancellableToken(ICancellationToken):
    @property
    def cancelled(self):
        return False

    def __or__(self, other: ICancellationToken):
        return self

    def __ror__(self, other: ICancellationToken):
        return self.__or__(other)

    def __ior__(self, value: ICancellationToken):
        return self.__or__(value)


NULL_CANCELLATION_TOKEN = _NullCancellationToken()
UNCANCELLABLE_TOKEN = _UncancellableToken()
