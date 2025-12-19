# stubs/aiokafka/producer/producer.pyi

import asyncio
from typing import Any, Callable, Sequence, Iterable, Literal, TypeVar
from types import TracebackType

_T = TypeVar("_T")


class AIOKafkaProducer:
    def __init__(
        self,
        *,
        bootstrap_servers: str | Sequence[str] = "localhost:9092",
        client_id: str | None = None,
        metadata_max_age_ms: int = 300000,
        request_timeout_ms: int = 40000,
        api_version: str = "auto",
        acks: Literal[0, 1, "all", -1] = 1,
        key_serializer: Callable[[Any], bytes] | None = None,
        value_serializer: Callable[[Any], bytes] | None = None,
        compression_type: Literal["gzip",
                                  "snappy", "lz4", "zstd"] | None = None,
        max_batch_size: int = 16384,
        partitioner: Callable[[bytes | None,
                               list[int], list[int]], int] | None = None,
        max_request_size: int = 1048576,
        linger_ms: int = 0,
        retry_backoff_ms: int = 100,
        security_protocol: Literal["PLAINTEXT", "SSL",
                                   "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT",
        ssl_context: Any | None = None,
        connections_max_idle_ms: int = 540000,
        enable_idempotence: bool = False,
        transactional_id: str | None = None,
        transaction_timeout_ms: int = 60000,
        sasl_mechanism: str = "PLAIN",
        sasl_plain_password: str | None = None,
        sasl_plain_username: str | None = None,
        **kwargs: Any
    ) -> None: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def flush(self) -> None: ...

    async def send(
        self,
        topic: str,
        value: Any = None,
        key: Any = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: Iterable[tuple[str, bytes]] | None = None
    ) -> asyncio.Future[Any]: ...

    async def send_and_wait(
        self,
        topic: str,
        value: Any = None,
        key: Any = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
        headers: Iterable[tuple[str, bytes]] | None = None
    ) -> Any: ...

    async def partitions_for(self, topic: str) -> set[int]: ...

    def create_batch(self) -> Any: ...

    async def send_batch(self, batch: Any, topic: str, *,
                         partition: int) -> asyncio.Future[Any]: ...

    async def begin_transaction(self) -> None: ...
    async def commit_transaction(self) -> None: ...
    async def abort_transaction(self) -> None: ...

    def transaction(self) -> "TransactionContext": ...

    async def __aenter__(self) -> "AIOKafkaProducer": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None: ...


class TransactionContext:
    def __init__(self, producer: AIOKafkaProducer) -> None: ...
    async def __aenter__(self) -> "TransactionContext": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None
    ) -> None: ...
