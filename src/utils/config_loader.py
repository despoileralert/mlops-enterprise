import yaml
import os
from typing import Dict, Any, Optional, Type, Union, List
from pathlib import Path
import logging
from pydantic import BaseModel, ValidationError, validator, Field
from pydantic.types import PositiveInt, constr
from enum import Enum

from utils.exception_handler import ConfigurationError

class LogLevel(str, Enum):
    """Valid log levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class Environment(str, Enum):
    """Valid environments."""
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"

class DatabaseConfig(BaseModel):
    """Database configuration schema."""
    host: str = Field(..., description="Database host")
    port: PositiveInt = Field(default=3306, description="Database port")
    database: str = Field(..., description="Database name")
    user: str = Field(..., description="Database user")
    password: str = Field(..., description="Database password")
    pool_size: PositiveInt = Field(default=10, le=50, description="Connection pool size")
    pool_name: str = Field(default="mlops_pool", description="Pool name")
    connection_timeout: PositiveInt = Field(default=30, description="Connection timeout in seconds")
    autocommit: bool = Field(default=False, description="Auto-commit transactions")
    charset: str = Field(default="utf8mb4", description="Database character set")
    collation: str = Field(default="utf8mb4_unicode_ci", description="Database collation")
    
    @validator('host')
    def validate_host(cls, v):
        if not v or v.strip() == "":
            raise ValueError('Host cannot be empty')
        return v.strip()
    
    @validator('password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v
    
    class Config:
        extra = "forbid"  # Prevent extra fields

class SparkConfig(BaseModel):
    """Spark configuration schema."""
    app_name: str = Field(default="MLOps-Spark", description="Spark application name")
    master: str = Field(default="local[*]", description="Spark master URL")
    driver_memory: str = Field(default="2g", description="Driver memory")
    executor_memory: str = Field(default="2g", description="Executor memory")
    executor_cores: PositiveInt = Field(default=2, description="Executor cores")
    max_result_size: str = Field(default="1g", description="Max result size")
    sql_adaptive_enabled: bool = Field(default=True, description="Enable adaptive query execution")
    sql_adaptive_coalesce_partitions_enabled: bool = Field(default=True, description="Enable adaptive coalescing")
    
    @validator('driver_memory', 'executor_memory', 'max_result_size')
    def validate_memory_format(cls, v):
        if not v.endswith(('g', 'G', 'm', 'M')):
            raise ValueError('Memory size must end with g/G (GB) or m/M (MB)')
        try:
            size = int(v[:-1])
            if size <= 0:
                raise ValueError('Memory size must be positive')
        except ValueError:
            raise ValueError('Invalid memory format')
        return v
    
    class Config:
        extra = "forbid"

class MLflowConfig(BaseModel):
    """MLflow configuration schema."""
    tracking_uri: str = Field(..., description="MLflow tracking server URI")
    artifact_root: str = Field(..., description="Artifact storage root")
    experiment_name: str = Field(default="default", description="Default experiment name")
    model_registry_uri: Optional[str] = Field(default=None, description="Model registry URI")
    
    @validator('tracking_uri')
    def validate_tracking_uri(cls, v):
        if not (v.startswith('http://') or v.startswith('https://') or v.startswith('file://')):
            raise ValueError('Tracking URI must start with http://, https://, or file://')
        return v
    
    class Config:
        extra = "forbid"

class LoggingConfig(BaseModel):
    """Logging configuration schema."""
    level: LogLevel = Field(default=LogLevel.INFO, description="Log level")
    format: str = Field(default="json", description="Log format")
    file_path: Optional[str] = Field(default=None, description="Log file path")
    max_file_size: str = Field(default="10MB", description="Max log file size")
    backup_count: PositiveInt = Field(default=5, description="Number of backup files")
    console_output: bool = Field(default=True, description="Enable console output")
    
    @validator('format')
    def validate_format(cls, v):
        if v not in ['json', 'text']:
            raise ValueError('Log format must be either "json" or "text"')
        return v
    
    class Config:
        extra = "forbid"

class MonitoringConfig(BaseModel):
    """Monitoring configuration schema."""
    drift_detection_enabled: bool = Field(default=True, description="Enable drift detection")
    performance_monitoring_enabled: bool = Field(default=True, description="Enable performance monitoring")
    alert_thresholds: Dict[str, float] = Field(default_factory=dict, description="Alert thresholds")
    report_frequency: str = Field(default="daily", description="Report frequency")
    
    @validator('report_frequency')
    def validate_frequency(cls, v):
        valid_frequencies = ['hourly', 'daily', 'weekly', 'monthly']
        if v not in valid_frequencies:
            raise ValueError(f'Report frequency must be one of: {valid_frequencies}')
        return v
    
    class Config:
        extra = "forbid"

class APIConfig(BaseModel):
    """API configuration schema."""
    host: str = Field(default="0.0.0.0", description="API host")
    port: PositiveInt = Field(default=8000, description="API port")
    workers: PositiveInt = Field(default=4, description="Number of workers")
    timeout: PositiveInt = Field(default=60, description="Request timeout")
    cors_enabled: bool = Field(default=True, description="Enable CORS")
    rate_limit_enabled: bool = Field(default=True, description="Enable rate limiting")
    
    class Config:
        extra = "forbid"

# ===== MAIN CONFIGURATION SCHEMAS =====

class BaseEnvironmentConfig(BaseModel):
    """Base configuration that all environments must have."""
    database: DatabaseConfig
    spark: SparkConfig
    mlflow: MLflowConfig
    logging: LoggingConfig
    monitoring: MonitoringConfig
    api: APIConfig
    environment: Environment
    
    class Config:
        extra = "forbid"

class DevConfig(BaseEnvironmentConfig):
    """Development environment configuration."""
    environment: Environment = Field(default=Environment.DEV, Literal=True)
    
    @validator('database')
    def validate_dev_database(cls, v):
        # Development-specific validations
        if not v.database.endswith('_dev'):
            v.database = f"{v.database}_dev"
        return v

class StagingConfig(BaseEnvironmentConfig):
    """Staging environment configuration."""
    environment: Environment = Field(default=Environment.STAGING, Literal=True)

class ProdConfig(BaseEnvironmentConfig):
    """Production environment configuration."""
    environment: Environment = Field(default=Environment.PROD, Literal=True)
    
    @validator('database')
    def validate_prod_database(cls, v):
        # Production-specific validations
        if v.pool_size < 10:
            raise ValueError('Production database pool size must be at least 10')
        return v
    
    @validator('logging')
    def validate_prod_logging(cls, v):
        # Production should not log to console by default
        if v.console_output:
            v.console_output = False
        return v

class ConfigLoader:
    """
    Multi-environment configuration management system.
    
    Features:
    - Environment-specific configurations
    - Environment variable substitution
    - Configuration validation
    - Hierarchical configuration merging
    - Hot reloading support
    """
    # Mapping of environments to their Pydantic models
    ENVIRONMENT_SCHEMAS = {
        Environment.DEV: DevConfig,
        Environment.STAGING: StagingConfig,
        Environment.PROD: ProdConfig
    }
    
    # Mapping of config types to their Pydantic models
    CONFIG_SCHEMAS = {
        'database': DatabaseConfig,
        'spark': SparkConfig,
        'mlflow': MLflowConfig,
        'logging': LoggingConfig,
        'monitoring': MonitoringConfig,
        'api': APIConfig
    }

    def __init__(self, config_dir: str = "config", environment: Optional[str] = None):
        """
        Initialize configuration loader.
        
        Args:
            config_dir: Directory containing configuration files
            environment: Target environment (dev/staging/prod)
        """
        self.config_dir = Path(config_dir)
        self.environment = environment or os.getenv("ENVIRONMENT", "dev")
        self.logger = logging.getLogger(__name__)
        self._config_cache = {}
    

    def load_config(self, config_name: str) -> Dict[str, Any]:
        """
        Load configuration file with environment override.
        
        Args:
            config_name: Name of configuration file (without .yaml extension)
            
        Returns:
            Merged configuration dictionary
            
        TODO: Implement configuration loading
        - Load base configuration file
        - Load environment-specific overrides
        - Merge configurations with proper precedence
        - Implement configuration caching
        - Add file change detection for hot reloading
        """
        base_config_path = self.config_dir / f"{config_name}.yaml"
        env_config_path = self.config_dir / f"{self.environment}_{config_name}.yaml"
        try:
            base_config = self.load_yaml_file(base_config_path)
            env_config = self.load_yaml_file(env_config_path)
            
            # Substitute environment variables
            base_config = self.substitute_environment_variables(base_config)
            
            # Merge configurations
            merged_config = self.merge_configs(base_config, env_config)
            
            # Validate configuration
            if not self.validate_config(merged_config, config_name):  # Replace {} with actual schema
                raise ConfigurationError("Configuration validation failed")
            
            return merged_config
        except FileNotFoundError as e:
            self.logger.error(f"Configuration file {base_config_path} or {env_config_path} not found: {e}")
            raise ConfigurationError(f"Configuration file not found: {e}") from e
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing configuration file {base_config_path} or {env_config_path}: {e}")
            raise ConfigurationError(f"Invalid YAML configuration: {e}") from e
    

    def load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Load and validate YAML file.
        
        Args:
            file_path: Path to YAML file
            
        Returns:
            Parsed YAML content
            
        TODO: Implement YAML loading
        - Load YAML file safely
        - Handle file not found errors
        - Validate YAML syntax
        - Return empty dict if file doesn't exist
        """
        try:
            with open(file_path, 'r') as f:
                return yaml.safe_load(f)
            
        except FileNotFoundError as e:
            self.logger.warning(f"Configuration file {file_path} not found. Returning empty configuration.")
            return {}
        
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML file {file_path}: {e}")
            raise ConfigurationError(f"Invalid YAML configuration: {file_path}") from e
        
        except Exception as e:
            self.logger.error(f"Unexpected error loading YAML file {file_path}: {e}")
            raise ConfigurationError(f"Error loading configuration: {file_path}") from e

    
    def substitute_environment_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Substitute environment variables in configuration values.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Configuration with substituted values
            
        TODO: Implement environment variable substitution
        - Find ${VAR_NAME} patterns in configuration values
        - Replace with environment variable values
        - Handle missing environment variables
        - Support default values (${VAR_NAME:default})
        - Process nested dictionaries recursively
        """

        envconfig = self.load_yaml_file(self.config_dir / 'env.yaml')
        try:
            if not isinstance(config, dict) or not isinstance(envconfig, dict):
                self.logger.error("Both config and envconfig must be dictionaries.")
                raise ConfigurationError("Invalid configuration format")
            self.merge_configs(config, envconfig)

        except Exception as e:
            self.logger.error("Error in merging the two configs")
            raise ConfigurationError("Error substituting environment variables") from e
        
        except RecursionError as e:
            self.logger.error("Infinite recursion loop reached while substituting environment variables.")
            raise ConfigurationError("Infinite recursion depth") from e
        
        return config
            

    def merge_configs(self, base_config: Dict[str, Any], override_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two configuration dictionaries.
        
        Args:
            base_config: Base configuration
            override_config: Override configuration
            
        Returns:
            Merged configuration

        """
        try:
            if not isinstance(base_config, dict) or not isinstance(override_config, dict):
                self.logger.error("Both base_config and override_config must be dictionaries.")
                raise ConfigurationError("Invalid configuration format")

            for key, value in override_config.items():
                if key in base_config and isinstance(base_config[key], dict) and isinstance(value, dict):
                    base_config[key] = self.merge_configs(base_config[key], value)
                else:
                    base_config[key] = value
            return base_config
        
        except RecursionError as e:
            self.logger.error("Inifnite recursion loop reached: {e}")
            raise ConfigurationError("Infinite recursion depth") from e
        
        except Exception as e:
            self.logger.error("Error merging dicts: {e}")
            raise ConfigurationError("Error merging configs") from e
        
    

    def validate_config(self, config: Dict[str, Any], schema_name: str) -> BaseModel:
        """
        Validate configuration against schema.
        
        Args:
            config: Configuration to validate
            schema: Validation schema
            
        Returns:
            True if valid, raises exception if not
            
        TODO: Implement configuration validation
        - Check required fields
        - Validate data types
        - Check value constraints
        - Provide detailed error messages
        """
        try:
            # Get the appropriate schema class
            if self.environment == 'environment':
                schema_class = self.ENVIRONMENT_SCHEMAS[self.environment]
            else:
                schema_class = self.CONFIG_SCHEMAS.get(schema_name)
                
            if not schema_class:
                raise ConfigurationError(
                    f"Unknown configuration schema: {schema_name}",
                    context={"available_schemas": list(self.CONFIG_SCHEMAS.keys())}
                )
            
            # Validate and create model instance
            validated_config = schema_class(**config['default'])
            
            self.logger.info(f"Successfully validated {schema_name} configuration")
            return validated_config
            
        except ValidationError as e:
            # Convert Pydantic validation errors to our custom exception
            error_details = []
            for error in e.errors():
                error_details.append({
                    'field': '.'.join(str(loc) for loc in error['loc']),
                    'message': error['msg'],
                    'type': error['type'],
                    'input': error.get('input')
                })
            print(error_details)
            
            raise ConfigurationError(
                f"Configuration validation failed for {schema_name}",
                context={
                    'validation_errors': error_details,
                    'error_count': len(error_details)
                }
            )
    

    def get_database_config(self) -> Dict[str, Any]:
        """
        Get database configuration with validation.
        
        Returns:
            Database configuration dictionary
            
        TODO: Implement database config loading
        - Load database.yaml configuration
        - Apply environment overrides
        - Validate required database parameters
        - Return ready-to-use configuration
        """
        try:
            dbconfigdict = self.load_config('database')
            if not dbconfigdict:
                raise ConfigurationError("Database configuration file is empty or missing.")
            return dbconfigdict
        except Exception as e:
            self.logger.error(f"Failed to load database configuration: {e}")
            raise ConfigurationError(f"Database configuration error: {e}") from e
    

    def get_spark_config(self) -> Dict[str, Any]:
        """
        Get Spark configuration.
        
        Returns:
            Spark configuration dictionary
            
        TODO: Implement Spark config loading
        - Load spark.yaml configuration
        - Handle driver and executor settings
        - Apply environment-specific tuning
        """
        try:
            sparkconfigdict = self.load_config('spark.yaml')
            if not sparkconfigdict:
                raise ConfigurationError("Spark configuration file is empty or missing.")
            return sparkconfigdict
        except Exception as e:
            self.logger.error(f"Failed to load Spark configuration: {e}")
            raise ConfigurationError(f"Spark configuration error: {e}") from e
    

    def get_mlflow_config(self) -> Dict[str, Any]:
        """
        Get MLflow configuration.
        
        Returns:
            MLflow configuration dictionary
            
        TODO: Implement MLflow config loading
        - Load mlflow.yaml configuration
        - Handle tracking server settings
        - Configure artifact storage
        """
        try:
            mlflowconfigdict = self.load_config('mlflow.yaml')
            if not mlflowconfigdict:
                raise ConfigurationError("MLFlow configuration file is empty or missing.")
            return mlflowconfigdict
        except Exception as e:
            self.logger.error(f"Failed to load MLFlow configuration: {e}")
            raise ConfigurationError(f"MLFlow configuration error: {e}") from e
        
        
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration with validation.
    
        Returns:
            Logging configuration dictionary
        """
        config = self.load_config('logging')
        
        # Validate required fields
        required_fields = ['level', 'format', 'handlers']
        for field in required_fields:
            if field not in config:
                raise ConfigurationError(f"Missing required logging config field: {field}")
        
        # Validate log level
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if config['level'].upper() not in valid_levels:
            raise ConfigurationError(f"Invalid log level: {config['level']}")
        
        return config
    

    def refresh_config(self, config_name: str):
        """
        Refresh cached configuration.
        
        Args:
            config_name: Name of configuration to refresh
            
        TODO: Implement configuration refresh
        - Clear configuration from cache
        - Reload from file
        - Notify dependent components
        """
        self._config_cache.pop(config_name, None)
        self.logger.info(f"Configuration {config_name} cache cleared.") 