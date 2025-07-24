import logging
import logging.handlers
import json
import uuid
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from pathlib import Path
from contextlib import contextmanager
import os


_thread_local = threading.local()

def set_correlation_id(correlation_id: str):
    """Set correlation ID for current thread."""
    _thread_local.correlation_id = correlation_id


def get_correlation_id() -> Optional[str]:
    """Get correlation ID for current thread."""
    return getattr(_thread_local, 'correlation_id', None)


def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())


class CorrelationFilter(logging.Filter):
    """
    Add correlation ID to log records for request tracking.
    
    TODO: Implement correlation ID filter
    - Extract correlation ID from context
    - Add to log record
    - Generate new ID if none exists
    """

    def filter(self, record):
        """Add correlation ID to log record."""
        correlation_id = get_correlation_id()
        if correlation_id is None:
            correlation_id = generate_correlation_id()
            set_correlation_id(correlation_id)
        record.correlation_id = correlation_id
        return True


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    
    TODO: Implement JSON formatting
    - Convert log records to JSON
    - Include standard fields (timestamp, level, message, etc.)
    - Handle exceptions and stack traces
    - Support custom fields
    """
    def __init__(self, fmt_dict: dict = None, time_format: str = "%Y-%m-%dT%H:%M:%S", msec_format: str = "%s.%03dZ"):
        self.fmt_dict = fmt_dict if fmt_dict is not None else {"message": "message"}
        self.default_time_format = time_format
        self.default_msec_format = msec_format
        self.datefmt = None

    def usesTime(self) -> bool:
        """
        Overwritten to look for the attribute in the format dict values instead of the fmt string.
        """
        return "asctime" in self.fmt_dict.values()

    def formatMessage(self, record) -> dict:
        """
        Overwritten to return a dictionary of the relevant LogRecord attributes instead of a string. 
        KeyError is raised if an unknown attribute is provided in the fmt_dict. 
        """
        return {fmt_key: record.__dict__[fmt_val] for fmt_key, fmt_val in self.fmt_dict.items()}

    def format(self, record) -> str:
        """
        Mostly the same as the parent's class method, the difference being that a dict is manipulated and dumped as JSON
        instead of a string.
        """
        record.message = record.getMessage()
        
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)

        message_dict = self.formatMessage(record)

        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            message_dict["exc_info"] = record.exc_text

        if record.stack_info:
            message_dict["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(message_dict, default=str)


class MLOpsLogger:
    """
    Centralized logging utility for MLOps operations.
    
    Features:
    - Structured JSON logging
    - Correlation ID support
    - Multiple output handlers
    - Environment-specific configuration
    - Performance-optimized
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize logging system.
        
        Args:
            config: Logging configuration
        """
        self.config = config
        self._loggers = {}
        self._setup_logging()
    
    def _setup_logging(self):
        """
        Setup logging configuration.

        """
        corr_filter = CorrelationFilter()
        json_formatter = JSONFormatter(fmt_dict=self.config.get("log_format", {"message": "message"}))
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s')

        # Console handler
        ch = logging.StreamHandler()
        ch.setlevel(self.config.get("console_log_level", logging.DEBUG))
        ch.setFormatter(console_formatter)
        logger.addHandler(ch)

        # File handler
        log_file = self.config.get("log_file", "mlops.log")
        log_dir = self.config.get("log_dir", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = Path(log_dir) / log_file
        fh = logging.handlers.RotatingFileHandler(
            log_path, 
            maxBytes=self.config.get("max_log_size", 10 * 1024 * 1024),  # 10 MB
            backupCount=self.config.get("backup_count", 5)
        )
        fh.setLevel(self.config.get("file_log_level", logging.INFO))
        fh.setFormatter(json_formatter)
        logger.addHandler(fh)

        #Cache the logger
        self._loggers["mlops"] = logging.getLogger("mlops")
        self._loggers["mlops"].setLevel(self.config.get("log_level", logging.INFO))
        self._loggers["mlops"].addFilter(corr_filter)  
        self._loggers["mlops"].propagate = False
        # Set up the main logger
        logger = logging.getLogger("mlops")
        logging.getLogger("mlops").addFilter(corr_filter)  
        logging.getLogger("mlops").setLevel(self.config.get("log_level", logging.INFO))
        logging.getLogger("mlops").addHandler(ch)
        logging.getLogger("mlops").addHandler(fh)
        logging.getLogger("mlops").propagate = False
        
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Get configured logger instance.
        
        Args:
            name: Logger name
            
        Returns:
            Configured logger instance
            
        """
        if name not in self._loggers:
            logger = logging.getLogger(name)
            logger.setLevel(self.config.get("log_level", logging.INFO))
            logger.addFilter(CorrelationFilter())
            json_formatter = JSONFormatter(fmt_dict=self.config.get("log_format", {"message": "message"}))
            for handler in logger.handlers:
                handler.setFormatter(json_formatter)
            self._loggers[name] = logger
        return self._loggers[name]
    
    def log_pipeline_start(self, pipeline_name: str, correlation_id: str, params: Dict[str, Any]):
        """
        Log pipeline execution start.
        
        Args:
            pipeline_name: Name of the pipeline
            correlation_id: Unique pipeline execution ID  
            params: Pipeline parameters
        """
        # TODO: Implement pipeline logging
        # Set correlation ID in context
        _thread_local.correlation_id = correlation_id
        
        logger = self.get_logger(f"mlops.pipeline.{pipeline_name}")
        
        logger.info(
            f"Pipeline '{pipeline_name}' started",
            extra={
                'event_type': 'pipeline_start',
                'pipeline_name': pipeline_name,
                'pipeline_id': correlation_id,
                'parameters': params,
                'start_time': datetime.now(timezone.utc).isoformat()
            }
        )
    
    def log_pipeline_end(self, pipeline_name: str, correlation_id: str, status: str, 
                        duration: float, metrics: Dict[str, Any]):
        """
        Log pipeline execution completion.
        
        Args:
            pipeline_name: Name of the pipeline
            correlation_id: Pipeline execution ID
            status: Execution status (success/failure/cancelled)
            duration: Execution duration in seconds
            metrics: Pipeline execution metrics
        """
        # TODO: Implement completion logging
        # Set correlation ID in context
        _thread_local.correlation_id = correlation_id
        
        logger = self.get_logger(f"mlops.pipeline.{pipeline_name}")
        
        # Choose log level based on status
        log_level = logging.INFO if status == 'success' else logging.ERROR
        
        logger.log(
            log_level,
            f"Pipeline '{pipeline_name}' completed with status: {status}",
            extra={
                'event_type': 'pipeline_end',
                'pipeline_name': pipeline_name,
                'pipeline_id': correlation_id,
                'status': status,
                'duration_seconds': duration,
                'duration_formatted': self._format_duration(duration),
                'metrics': metrics,
                'end_time': datetime.now(timezone.utc).isoformat()
            }
        )
    
    def log_model_training(self, model_name: str, experiment_id: str, metrics: Dict[str, Any]):
        """
        Log model training events.
        
        Args:
            model_name: Name of the model
            experiment_id: MLflow experiment ID
            metrics: Training metrics
        """
        # TODO: Implement model training logs
        logger = self.get_logger(f"mlops.model.{model_name}")
        
        logger.info(
            f"Model '{model_name}' training completed",
            extra={
                'event_type': 'model_training',
                'model_name': model_name,
                'experiment_id': experiment_id,
                'training_metrics': metrics,
                'training_time': datetime.now(timezone.utc).isoformat()
            }
        )
    
    def log_prediction_request(self, model_id: str, input_features: Dict[str, Any], 
                             prediction: Any, latency: float):
        """
        Log prediction requests for monitoring.
        
        Args:
            model_id: Model identifier
            input_features: Input features (consider privacy)
            prediction: Model prediction
            latency: Prediction latency in seconds
        """
        # TODO: Implement prediction logging
        logger = self.get_logger("mlops.prediction")
        
        # Sanitize input features for privacy
        sanitized_features = self._sanitize_features(input_features)
        
        logger.info(
            f"Prediction request processed for model {model_id}",
            extra={
                'event_type': 'prediction_request',
                'model_id': model_id,
                'input_features': sanitized_features,
                'prediction': prediction,
                'latency_ms': latency * 1000,
                'prediction_time': datetime.now(timezone.utc).isoformat()
            }
        )
    
    def log_data_quality_check(self, dataset_name: str, quality_metrics: Dict[str, Any], 
                              passed: bool):
        """
        Log data quality check results.
        
        Args:
            dataset_name: Name of the dataset
            quality_metrics: Quality check metrics
            passed: Whether quality checks passed
        """
        logger = self.get_logger("mlops.data_quality")
        
        log_level = logging.INFO if passed else logging.WARNING
        
        logger.log(
            log_level,
            f"Data quality check for '{dataset_name}': {'PASSED' if passed else 'FAILED'}",
            extra={
                'event_type': 'data_quality_check',
                'dataset_name': dataset_name,
                'quality_metrics': quality_metrics,
                'passed': passed,
                'check_time': datetime.now(timezone.utc).isoformat()
            }
        )
    
    def log_model_drift(self, model_id: str, drift_metrics: Dict[str, Any], 
                       drift_detected: bool):
        """
        Log model drift detection results.
        
        Args:
            model_id: Model identifier
            drift_metrics: Drift detection metrics
            drift_detected: Whether drift was detected
        """
        logger = self.get_logger("mlops.drift")
        
        log_level = logging.WARNING if drift_detected else logging.INFO
        
        logger.log(
            log_level,
            f"Drift detection for model {model_id}: {'DRIFT DETECTED' if drift_detected else 'NO DRIFT'}",
            extra={
                'event_type': 'drift_detection',
                'model_id': model_id,
                'drift_metrics': drift_metrics,
                'drift_detected': drift_detected,
                'detection_time': datetime.now(timezone.utc).isoformat()
            }
        )


@contextmanager
def setup_correlation_context(correlation_id: Optional[str] = None):
    """
    Setup correlation ID context for request tracking.
    
    Args:
        correlation_id: Existing correlation ID or None to generate
        
    Returns:
        Correlation ID

    Context manager for correlation ID management.
    """
    # Store previous correlation ID (if any)
    previous_id = get_correlation_id()
    
    try:
        # Set new correlation ID
        if correlation_id is None:
            correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)
        
        yield correlation_id
        
    finally:
        # Restore previous correlation ID
        if previous_id is not None:
            set_correlation_id(previous_id)
        else:
            # Clear correlation ID if none existed before
            if hasattr(_thread_local, 'correlation_id'):
                delattr(_thread_local, 'correlation_id')
    pass

def get_correlation_id() -> Optional[str]:
    """
    Get current correlation ID from context.
    
    Returns:
        Current correlation ID or None
        
    TODO: Implement context retrieval
    - Get ID from thread-local storage
    - Handle missing context gracefully
    """
    return getattr(_thread_local, 'correlation_id', None)