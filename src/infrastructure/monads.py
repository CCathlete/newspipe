# src/infrastructure/monads.py

from dataclasses import dataclass
from returns.maybe import (
    Maybe as _Maybe,
    Some as _Some,
    Nothing as _Nothing

)


from returns.result import (
    Result as _Result,
    Success as _Success,
    Failure as _Failure
)

from typing import Generic, TypeVar, Callable, Protocol

# Define type variables
T = TypeVar(name='T')  # T is a variable that holds the generic type called T.
U = TypeVar(name='U')
E = TypeVar(name='E', bound=Exception)


Success = _Success
Failure = _Failure
Maybe = _Maybe
Some = _Some
Nothing = _Nothing


# Define the interface for our Result type
# T is the type of an original Result and U is the type of the of a callable's
# output if Success.

class IResult(Protocol[T, E]):
    """Interface for a Result type with proper typing for bind operations."""

    def bind(self, func: Callable[[T], 'IResult[U, E]']) -> 'IResult[U, E]':
        """Monadic bind operation with proper type inference."""
        ...

    def map(self, func: Callable[[T], U]) -> 'IResult[U, E]':
        """Functor map operation."""
        ...

    @classmethod
    def success(cls, value: T) -> 'IResult[T, E]':
        """Create a successful result."""
        ...

    @classmethod
    def failure(cls, error: E) -> 'IResult[T, E]':
        """Create a failed result."""
        ...

# Create a concrete implementation that wraps returns.Result


@dataclass(slots=True, frozen=True)
class Result(Generic[T, E]):
    """Wrapper around returns.Result that implements our Result interface."""
    _result: _Result[T, E]

    def bind(self, func: Callable[[T], IResult[U, E]]) -> IResult[U, E]:
        """Bind operation with proper type inference."""
        return Result(self._result.bind(lambda x: func(x).unwrap()))  # type: ignore

    def map(self, func: Callable[[T], U]) -> IResult[U, E]:
        """Map operation."""
        return Result(self._result.map(func))

    @classmethod
    def success(cls, value: T) -> IResult[T, E]:
        """Create a successful result."""
        return cls(Success(value))  # type: ignore

    @classmethod
    def failure(cls, error: E) -> IResult[T, E]:
        """Create a failed result."""
        return cls(Failure.failure(error))  # type: ignore

    def unwrap(self) -> _Result[T, E]:
        """Get the underlying returns.Result."""
        return self._result
