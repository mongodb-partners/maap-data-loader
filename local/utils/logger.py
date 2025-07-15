import sys
import config
import asyncio
import logging
import os
import time
import inspect
import functools
from contextlib import contextmanager
from httpx import AsyncClient, Client, HTTPStatusError
from datetime import datetime, timezone



class MaapLogger:
    """
    A singleton asynchronous and synchronous remote logger that logs messages both locally and to a remote service.
    Supports logging with dynamic context fields and structured metadata.
    Attributes:
        service_url (str): The URL of the remote logging service.
        app_name (str): The name of the application using the logger.
        log_dir (str): The directory where local log files are stored.
        async_client (AsyncClient): The async HTTP client used for sending logs to the remote service.
        sync_client (Client): The sync HTTP client used for sending logs to the remote service.
        local_logger (logging.Logger): The local fallback logger.
        context (Dict): Thread/task local context that will be included in all logs.
    """
    _instance = None
    _thread_local_context = {}  # Simple thread-local context storage
    
    def __new__(cls, *args, **kwargs):
        # Ensure only one instance of the logger is created (singleton pattern)
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._init_logger(*args, **kwargs)
        return cls._instance

    def _init_logger(self, service_url: str, app_name: str, log_dir: str = "logs", 
                    log_level: str = "INFO", retention_days: int = 3):
        """
        Initialize the logger with the given parameters.
        
        Args:
            service_url: URL of the remote logging service
            app_name: Name of the application using the logger
            log_dir: Directory where local log files are stored
            log_level: Default logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            retention_days: Number of days to keep local log files
        """
        self.service_url = service_url
        self.app_name = app_name
        self.async_client = AsyncClient(timeout=300.0)
        self.sync_client = Client(timeout=300.0)
        self.log_dir = log_dir
        self.retention_days = retention_days
        os.makedirs(self.log_dir, exist_ok=True)
        self.cleanup_old_logs()
        
        # Set up the local logger
        self.local_logger = logging.getLogger(app_name)
        log_level_num = getattr(logging, log_level.upper(), logging.INFO)
        self.local_logger.setLevel(log_level_num)
        
        # Check if handlers already exist to prevent duplicate handlers
        if not self.local_logger.handlers:
            # File handler
            file_handler = logging.FileHandler(os.path.join(self.log_dir, f"{app_name}.log"))
            file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(file_formatter)
            self.local_logger.addHandler(file_handler)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter("%(levelname)s: %(message)s")
            console_handler.setFormatter(console_formatter)
            self.local_logger.addHandler(console_handler)

    def cleanup_old_logs(self):
        """Remove log files older than the configured retention period."""
        now = time.time()
        cutoff_time = now - (self.retention_days * 24 * 60 * 60)
        
        for filename in os.listdir(self.log_dir):
            file_path = os.path.join(self.log_dir, filename)
            if os.path.isfile(file_path):
                file_modified_time = os.path.getmtime(file_path)
                if file_modified_time < cutoff_time:
                    try:
                        os.remove(file_path)
                        print(f"Removed old log file: {file_path}")
                    except Exception as e:
                        print(f"Error removing file {file_path}: {e}")

    @contextmanager
    def with_context(self, **context_values):
        """
        Context manager that adds context values to all logs within its scope.
        
        Example:
            with logger.with_context(user_id="123", conversation_id="abc"):
                logger.info("User action performed")  # Will include user_id and conversation_id
        """
        # Store the previous context
        previous_context = self._thread_local_context.copy()
        
        # Update with new context values
        self._thread_local_context.update(context_values)
        
        try:
            yield
        finally:
            # Restore the previous context
            self._thread_local_context = previous_context

    def _get_caller_info(self):
        """Get information about the calling function for enhanced logging."""
        stack = inspect.stack()
        # Skip this function, its caller, and the logging function
        # to find the actual user code that called the logger
        frame = stack[3] if len(stack) > 3 else stack[-1]
        filename = os.path.basename(frame.filename)
        lineno = frame.lineno
        function = frame.function
        return {
            "caller_file": filename,
            "caller_line": lineno,
            "caller_function": function
        }

    def _prepare_log_payload(self, level: str, message: str, **fields):
        """Prepare the payload for logging."""
        # Log locally with context
        context_str = ""
        if self._thread_local_context or fields:
            context_parts = []
            
            # Add thread-local context
            for k, v in self._thread_local_context.items():
                if v is not None:
                    context_parts.append(f"{k}={v}")
            
            # Add fields passed to this log call
            for k, v in fields.items():
                if v is not None:
                    context_parts.append(f"{k}={v}")
            
            if context_parts:
                context_str = f" [{', '.join(context_parts)}]"
        
        # Prepare the payload for remote logging
        payload = {
            "level": level.upper(),  # Ensure level is uppercase
            "message": str(message),
            "app_name": self.app_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),  # ISO format for consistency
        }
        
        # Include any thread-local context values as top-level fields
        for key, value in self._thread_local_context.items():
            if value is not None:  # Only include non-None values
                payload[key] = value
        
        # Include additional fields passed to this log call as top-level fields
        for key, value in fields.items():
            if value is not None:  # Only include non-None values
                payload[key] = value
        
        # Include caller information in debug/error logs for tracing
        if level.upper() in ["DEBUG", "ERROR", "CRITICAL"]:
            caller_info = self._get_caller_info()
            for key, value in caller_info.items():
                payload[key] = value
        
        return payload, context_str

    # Asynchronous logging methods
    async def alog(self, level: str, message: str, **fields):
        """
        Log a message asynchronously with the specified level and additional fields.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: The log message
            **fields: Additional fields to include in the log
        """
        log_method = getattr(self.local_logger, level.lower(), None)
        if not log_method:
            self.local_logger.error(f"Invalid log level: {level}. Message: {message}")
            return

        payload, context_str = self._prepare_log_payload(level, message,  **fields)
        
        # Log locally with context
        log_method(f"{message}{context_str}")
        
        try:
            # Send the log to the remote service
            response = await self.async_client.post(
                f"{self.service_url}/log",
                json=payload,
                timeout=5.0  # Shorter timeout for logging
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            self.local_logger.error(f"HTTP error during remote logging: {e}")
        except Exception as e:
            self.local_logger.error(f"Error sending log to the remote service: {e}")

    async def ainfo(self, message: str, **fields):
        """Log an informational message asynchronously with additional fields."""
        await self.alog("INFO", message, **fields)

    async def adebug(self, message: str, **fields):
        """Log a debug message asynchronously with additional fields."""
        await self.alog("DEBUG", message, **fields)

    async def awarning(self, message: str, **fields):
        """Log a warning message asynchronously with additional fields."""
        await self.alog("WARNING", message, **fields)

    async def aerror(self, message: str, **fields):
        """Log an error message asynchronously with additional fields."""
        await self.alog("ERROR", message, **fields)

    async def acritical(self, message: str, **fields):
        """Log a critical message asynchronously with additional fields."""
        await self.alog("CRITICAL", message, **fields)

    async def aprint(self, *args, sep=" ", end="\n", **fields):
        """
        Log a message with the INFO level, formatted like the built-in print function.
        Additional fields will be included in the log.
        """
        message = sep.join(map(str, args)) + end.strip()
        print(message, end=end)
        await self.ainfo(message, **fields)

    async def aclose(self):
        """Close the async HTTP client used for remote logging."""
        await self.async_client.aclose()

    # Synchronous logging methods
    def log(self, level: str, message: str, **fields):
        """
        Log a message synchronously with the specified level and additional fields.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: The log message
            **fields: Additional fields to include in the log
        """
        log_method = getattr(self.local_logger, level.lower(), None)
        if not log_method:
            self.local_logger.error(f"Invalid log level: {level}. Message: {message}")
            return

        payload, context_str = self._prepare_log_payload(level, message, **fields)
        
        # Log locally with context
        log_method(f"{message}{context_str}")
        
        try:
            # Send the log to the remote service synchronously
            response = self.sync_client.post(
                f"{self.service_url}/log",
                json=payload,
                timeout=300.0
            )
            response.raise_for_status()
        except HTTPStatusError as e:
            self.local_logger.error(f"HTTP error during remote logging: {e}")
        except Exception as e:
            self.local_logger.error(f"Error sending log to the remote service: {e}")

    def info(self, message: str, **fields):
        """Log an informational message synchronously with additional fields."""
        self.log("INFO", message, **fields)

    def debug(self, message: str, **fields):
        """Log a debug message synchronously with additional fields."""
        self.log("DEBUG", message, **fields)

    def warning(self, message: str, **fields):
        """Log a warning message synchronously with additional fields."""
        self.log("WARNING", message, **fields)

    def error(self, message: str, **fields):
        """Log an error message synchronously with additional fields."""
        self.log("ERROR", message, **fields)

    def critical(self, message: str, **fields):
        """Log a critical message synchronously with additional fields."""
        self.log("CRITICAL", message, **fields)

    def print(self, *args, sep=" ", end="\n", **fields):
        """
        Log a message synchronously with the INFO level, formatted like print.
        Additional fields will be included in the log.
        """
        message = sep.join(map(str, args)) + end.strip()
        print(message, end=end)
        self.info(message, **fields)

    def close(self):
        """Synchronously close the HTTP client used for remote logging."""
        self.sync_client.close()
        # We need to ensure the async client is also closed
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(self.aclose())
        except RuntimeError:
            # No running event loop
            asyncio.run(self.aclose())

    # Utility methods
    def log_function(self, func):
        """
        Decorator to log function entry and exit with parameters and return values.
        
        Example:
            @logger.log_function
            def my_function(a, b):
                return a + b
        """
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            func_name = func.__name__
            # Log function entry
            params = ", ".join([f"{arg}" for arg in args] + [f"{k}={v}" for k, v in kwargs.items()])
            await self.adebug(f"ENTER {func_name}({params})", function=func_name)
            
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                # Log successful function exit
                await self.adebug(f"EXIT {func_name} -> {result} ({duration:.3f}s)", 
                                function=func_name, duration=f"{duration:.3f}")
                return result
            except Exception as e:
                duration = time.time() - start_time
                # Log function exception
                await self.aerror(f"EXCEPTION in {func_name}: {type(e).__name__}: {e} ({duration:.3f}s)", 
                                function=func_name, error_type=type(e).__name__, duration=f"{duration:.3f}")
                raise

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            func_name = func.__name__
            # Log function entry
            params = ", ".join([f"{arg}" for arg in args] + [f"{k}={v}" for k, v in kwargs.items()])
            self.debug(f"ENTER {func_name}({params})", function=func_name)
            
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                # Log successful function exit
                self.debug(f"EXIT {func_name} -> {result} ({duration:.3f}s)", 
                          function=func_name, duration=f"{duration:.3f}")
                return result
            except Exception as e:
                duration = time.time() - start_time
                # Log function exception
                self.error(f"EXCEPTION in {func_name}: {type(e).__name__}: {e} ({duration:.3f}s)", 
                          function=func_name, error_type=type(e).__name__, duration=f"{duration:.3f}")
                raise

        # Choose the appropriate wrapper based on whether the function is async or not
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
        

def get_logger():
    """Get a singleton logger instance."""
    try:
        return MaapLogger(service_url=config.LOGGER_SERVICE_URL, app_name=config.APP_NAME)
    except Exception:
        # Fallback to basic logging if MAAP logger is not available
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            stream=sys.stdout
        )
        return logging.getLogger(config.APP_NAME)

# Export logger for convenience
logger = get_logger()