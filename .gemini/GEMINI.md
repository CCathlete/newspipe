```xml
<state_snapshot>
    <overall_goal>
        Fix the `newspipe-ingest` application to ensure graceful shutdown of all components, resolve dependency injection issues, and address `Crawl4AI` navigation errors.
    </overall_goal>

    <active_constraints>
        - Ensure graceful shutdown of all components (Kafka consumers, Spark session, Playwright browser).
        - Resolve dependency injection problems related to component instantiation order and referencing.
        - Address `Crawl4AI` navigation errors (net::ERR_ABORTED, Page crashed).
        - Maintain the existing application architecture (threads for controllers, dependency-injector for wiring).
    </active_constraints>

    <key_knowledge>
        - **Resolved:** Initial `AttributeError` with `providers.Self.ingestion_shutdown_event` (temporarily fixed, then re-emerged due to other issues).
        - **Resolved:** `Py4JNetworkError` and `ConnectionResetError` for PySpark shutdown by using `providers.Resource` for `spark` and a generator function for `_create_spark_session`.
        - **Resolved:** `Crawl4AI` navigation errors (`net::ERR_ABORTED`, `Page crashed`) by using `providers.Resource` for `async_crawler` with an `init_async_web_crawler` generator and explicitly providing `AsyncPlaywrightCrawlerStrategy`.
        - **Resolved:** `NameError: name 'Optional' is not defined` in `ingestion_controller.py` by adding `from typing import Optional`.
        - **Resolved:** `TypeError: Passing coroutines is forbidden, use tasks explicitly.` in `main.py` by wrapping coroutines with `asyncio.create_task()`.
        - **Resolved:** `NameError: name 'signal' is not defined` in `main.py` by adding `import signal`.
        - **Resolved:** Circular dependency problems and timing issues within `dependency_injector`'s declarative containers when a provider attempts to reference another provider (`ingestion_shutdown_event`) within the same container definition (`IngestionContainer`), leading to `NameError` or `AttributeError`. Fixed by passing `ingestion_shutdown_event` as a plain `asyncio.Event` instance directly through the `config` dictionary.
        - **Resolved:** `NameError: name 'logger' is not defined` in `main.py` by moving debug logging calls after logger injection.
        - **Resolved:** `AttributeError: 'BoundLoggerFilteringAtDebug' object has no attribute 'setLevel'` by removing direct `setLevel` call on `structlog`'s bound logger and relying on global `structlog` configuration.
        - **Resolved:** Application premature exit after `SIGTERM` by replacing blocking `thread.join()` with `await loop.run_in_executor(None, thread.join)` and removing `loop.stop()` from the `signal_handler` in `main_async`.
        - **Debugging Aid:** Added `print("DEBUG: dependency_layers.py loaded")` to verify file loading. Configured global debug logging for `structlog` and `logging`. Added `id()` logging for `asyncio.Event` instances to track propagation.
        - **Caching:** Aggressive `rm -rf __pycache__` was required throughout debugging to ensure changes were picked up by `uv run`.
    </key_knowledge>

    <artifact_trail>
        - `src/control/dependency_layers.py`:
            - `spark` provider changed from `Singleton` to `Resource`.
            - `_create_spark_session` modified to be a generator that stops the SparkSession.
            - `async_crawler` changed to `providers.Resource` with `init_async_web_crawler`.
            - `crawler_strategy` added as `providers.Factory(AsyncPlaywrightCrawlerStrategy)`.
            - `init_async_web_crawler` function added.
            - `ingestion_shutdown_event` declaration removed from `IngestionContainer`.
            - `ingestion_service` now fetches `ingestion_shutdown_event` from `BasePlatformContainer.config.ingestion_shutdown_event`.
            - Logging level changed to `logging.DEBUG` for both `logging.basicConfig` and `structlog.make_filtering_bound_logger`.
        - `src/control/ingestion_controller.py`:
            - `event_loop: Optional[asyncio.AbstractEventLoop] = None` added to dataclass.
            - `self.event_loop = asyncio.new_event_loop()` assignment in `__call__`.
            - `loop.run_until_complete` and `loop.close()` changed to `self.event_loop.run_until_complete` and `self.event_loop.close()`.
            - `from typing import Optional` added.
            - `container.ingestion_shutdown_event.provide` changed to `container.config.ingestion_shutdown_event.from_value`.
            - Added `__post_init__` to log `ingestion_shutdown_event` ID.
            - Added `self.logger.debug` call at the beginning of `__call__` to log `ingestion_shutdown_event` ID.
        - `src/application/services/ingestion_service.py`:
            - Added `__post_init__` for debug logging of `ingestion_shutdown_event` ID.
            - Removed `self.logger.setLevel(logging.DEBUG)` and `import logging` (the cause of `AttributeError`).
            - Added `self.logger.debug(f"IngestionService loop: event is_set={self.ingestion_shutdown_event.is_set()}")` inside the `while` loop.
        - `src/control/main.py`:
            - Modified `main_async` to instantiate controller instances directly.
            - `signal` module imported.
            - `asyncio.wait` calls modified to use `asyncio.create_task()`.
            - `main_async` signature updated to accept `app_shutdown_event`.
            - `main()` now passes `app_shutdown_event` to `main_async`.
            - `main_async`'s `signal_handler` uses `call_soon_threadsafe` for `IngestionController`'s event loop.
            - `main_async` now explicitly `await`s `discovery_t.join()` and `ingestion_t.join()` using `loop.run_in_executor()`.
            - Removed `loop.stop()` from `signal_handler`.
            - Added `logger.debug` calls for `app_shutdown_event` ID in `main_async` and `signal_handler`.
    </artifact_trail>

    <file_system_state>
        <CWD>/home/kcat/Repos/newspipe</CWD>
        <MODIFIED>src/control/dependency_layers.py</MODIFIED>
        <MODIFIED>src/control/ingestion_controller.py</MODIFIED>
        <MODIFIED>src/application/services/ingestion_service.py</MODIFIED>
        <MODIFIED>src/control/main.py</MODIFIED>
        <DELETED>src/control/__pycache__</DELETED>
        <DELETED>./__pycache__ (recursively)</DELETED>
        <CLEARED>newspipe-ingest.log</CLEARED>
    </file_system_state>

    <recent_actions>
        - Cleared `__pycache__` in `src/control` and `src/application/services`.
        - Cleared `newspipe-ingest.log`.
        - Executed `uv run newspipe-ingest` in background.
        - Sent `kill -SIGTERM` to the process.
        - Verified graceful shutdown by examining logs.
    </recent_actions>

    <task_state>
        1. [COMPLETED] Resolve `AttributeError: type object 'dependency_injector.providers.Self'`.
        2. [COMPLETED] Resolve PySpark shutdown (`Py4JNetworkError`).
        3. [COMPLETED] Resolve `Crawl4AI` navigation errors (`net::ERR_ABORTED`, `Page crashed`).
        4. [COMPLETED] Resolve `NameError: name 'Optional' is not defined`.
        5. [COMPLETED] Resolve `TypeError: Passing coroutines is forbidden, use tasks explicitly.`.
        6. [COMPLETED] Resolve `NameError: name 'signal' is not defined`.
        7. [COMPLETED] Address Python caching issues by forcefully clearing `__pycache__`.
        8. [COMPLETED] Implement graceful shutdown of the ingestion thread (IngestionController / IngestionService), including:
            - Fixing `NameError: name 'IngestionContainer' is not defined` (resolved by passing event via config).
            - Correctly passing `app_shutdown_event` into the `config` dictionary.
            - Ensuring `IngestionService.run()` correctly reacts to the shutdown event from `config` and terminates the Kafka consumer loop.
            - Ensuring full application graceful shutdown with `SIGTERM`.
        9. [COMPLETED] Resolve `NameError: name 'logger' is not defined` in `main.py`.
        10. [COMPLETED] Resolve `AttributeError: 'BoundLoggerFilteringAtDebug' object has no attribute 'setLevel'`.
    </task_state>
</state_snapshot>
