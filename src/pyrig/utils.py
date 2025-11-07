import asyncio
import json
import socket

import structlog
import zmq
import zmq.asyncio


INCLUDE_CALLSITE_IN_LOGS = True


def add_timestamp_if_missing(logger, method_name, event_dict):
    """Only add timestamp if not already present (forwarded logs)."""
    if "timestamp" not in event_dict:
        return structlog.processors.TimeStamper(fmt="iso", utc=True)(logger, method_name, event_dict)
    return event_dict


def add_level_if_missing(logger, method_name, event_dict):
    """Only add level if not already present (forwarded logs)."""
    if "level" not in event_dict:
        return structlog.processors.add_log_level(logger, method_name, event_dict)
    return event_dict


def _get_base_processors() -> list:
    """Get base structlog processors.

    Uses conditional processors that only add timestamp/level if missing,
    so they work for both local logs and forwarded logs from nodes.

    Returns:
        List of structlog processors
    """
    processors = [
        structlog.contextvars.merge_contextvars,
        add_level_if_missing,
        add_timestamp_if_missing,
    ]

    if INCLUDE_CALLSITE_IN_LOGS:
        processors.insert(
            1,
            structlog.processors.CallsiteParameterAdder(
                [
                    structlog.processors.CallsiteParameter.MODULE,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                ]
            ),
        )

    return processors


def configure_console_logging(level: str = "INFO"):
    """Configure structlog for the rig with console output.

    Uses conditional processors to preserve timestamps/levels from forwarded node logs
    while still adding them to locally-generated logs.

    Args:
        level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    structlog.configure(
        processors=_get_base_processors() + [structlog.dev.ConsoleRenderer()],
        wrapper_class=structlog.make_filtering_bound_logger(level.lower()),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


class ZMQLogger:
    """Logger that sends messages over ZMQ."""

    def __init__(self, socket: zmq.Socket):
        self.socket = socket

    def msg(self, *args, **kwargs) -> None:
        """Send the log message as JSON over ZMQ.

        Structlog passes the final event_dict as keyword arguments.
        """
        try:
            # Reconstruct the event dict from kwargs
            # The first positional arg (if any) is the message/event
            event_dict = dict(kwargs)
            if args:
                event_dict["event"] = args[0]
            self.socket.send_json(event_dict)
        except Exception:
            # Don't let logging errors break the application
            pass

    def __getattr__(self, name: str):
        """All log methods (debug, info, etc.) use msg()."""
        return self.msg


def configure_zmq_logging(zmq_socket: zmq.Socket):
    """Configure structlog for a node with ZMQ transport.

    This configures structlog to send all log messages over a ZMQ PUB socket
    to a central log aggregator. Logs are fully processed (timestamped, leveled)
    before being sent.

    Args:
        zmq_socket: ZMQ PUB socket connected to log aggregator
    """
    structlog.configure(
        processors=_get_base_processors(),
        context_class=dict,
        logger_factory=lambda: ZMQLogger(zmq_socket),
        cache_logger_on_first_use=False,
    )


async def run_log_aggregator(log_socket: zmq.asyncio.Socket, logger: structlog.BoundLogger):
    """Background task to receive JSON logs from nodes and forward to structlog.

    This aggregator receives fully-processed log events from nodes and re-emits them
    through the local structlog configuration. The events have already been processed
    by the sender's processor chain (timestamped, contextvars merged, etc).

    Args:
        log_socket: ZMQ SUB socket bound to log port
        logger: Structlog logger to forward messages to
    """
    try:
        logger = structlog.get_logger(__name__)
        logger.debug("log_aggregator_started", endpoint=log_socket.getsockopt(zmq.LAST_ENDPOINT))
        while True:
            # Receive fully-processed log event from node
            event_dict = await log_socket.recv_json()

            # Extract the log level to call the appropriate method
            level = event_dict.get("level", "info").lower()
            event = event_dict.get("event", "")

            # Extract all context (everything except 'event')
            context = {k: v for k, v in event_dict.items() if k != "event"}

            # Re-emit through local logger with original context
            # This will go through the console renderer for display
            log_method = getattr(logger, level, logger.info)
            log_method(event, **context)

    except asyncio.CancelledError:
        pass  # Task cancelled during shutdown
    except json.JSONDecodeError as e:
        logger.error("log_decode_error", error=str(e))
    except Exception as e:
        logger.error("log_aggregator_error", error=str(e))


def get_local_ip() -> str:
    """Get local IP address."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.1)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"
