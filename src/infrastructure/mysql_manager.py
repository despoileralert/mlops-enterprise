import mysql.connector
from mysql.connector import pooling, Error
from typing import Optional, Dict, Any, List, Tuple
import logging
from contextlib import contextmanager

from utils.config_loader import ConfigLoader
from utils.exception_handler import DatabaseConnectionError, DatabaseOperationError

class MySQLManager:
    """
    Enterprise-grade MySQL connection manager with pooling, health checks, and transaction support.
    
    Features:
    - Connection pooling for scalability
    - Health check functionality
    - Transaction management
    - Parameterized queries for security
    - Automatic retry logic
    - Connection lifecycle management
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MySQL manager with configuration.
        
        Args:
            config: Database configuration dictionary
        """
        self.config = config
        self.pool = None
        self.logger = logging.getLogger(__name__)
        self.initialize_pool()


    def initialize_pool(self):
        """
        Initialize connection pool with retry logic.
        This method attempts to create a connection pool with the provided configuration.
        """

        retryconfig = self.config['retry']
        max_retries = retryconfig['connection_retries']
        retry_delay = retryconfig['retry_delay']

        for retry in range(max_retries):
            try:
                poolconfig = self.config['default']
                self.pool = mysql.connector.pooling.MySQLConnectionPool(**poolconfig)
                self.logger.info(
                    f"MySQL connection pool initialized successfully: "
                    f"pool_name={poolconfig['pool_name']}, "
                    f"pool_size={poolconfig['pool_size']}, "
                    f"host={poolconfig['host']}:{poolconfig['port']}"
                )

            except mysql.connector.Error as e:
                self.logger.error(f"MySQL connection attempt {retry + 1} failed: {e}",
                                  extra={
                                        'error_code': e.errno,
                                        'sql_state': getattr(e, 'sqlstate', None),
                                        'attempt': retry + 1,
                                        'max_retries': max_retries
                                    }
                                )

                if retry == max_retries - 1:
                    self.logger.error(
                        f"Failed to initialize MySQL connection pool after {max_retries} attempts: {e}"
                    )
                    raise DatabaseConnectionError(f"Could not connect to MySQL database: {e}")

        return self.pool


    def getconnection(self) -> mysql.connector.connection:
        """
        Get connection from pool with error handling.
        
        Returns:
            mysql.connector.connection: Database connection
        """
        try:
            connection = self.pool.get_connection()

            if not connection.is_connected():
                self.logger.error("Connection from pool is not active, attempting to reinitialize pool.")
                connection.close() 
                self.initialize_pool()
                connection = self.pool.get_connection()

            self.logger.info("Connection successfully retrieved from pool.")
            print("Connection successfully retrieved from pool.")
            return connection

        except mysql.connector.errors.PoolError as e:
            self.logger.error(f"Connection pool exhausted: {e}",
                              extra={
                                  'error_code': e.errno,
                                  'sql_state': getattr(e, 'sqlstate', None)
                              })
            raise DatabaseConnectionError(f"Could not retrieve connection from MySQL pool: {e}")
        
        except mysql.connector.Error as e:
            self.logger.error(f"Error retrieving connection from pool: {e}",
                              extra={
                                  'error_code': e.errno,
                                  'sql_state': getattr(e, 'sqlstate', None)
                              })
            raise DatabaseConnectionError(f"Could not retrieve connection from MySQL pool: {e}")
        
        except Exception as e:
            self.logger.error(f"Unexpected error retrieving connection from pool: {e}",
                              extra={
                                  'error_code': getattr(e, 'errno', None),
                                  'sql_state': getattr(e, 'sqlstate', None)
                              })
            raise DatabaseConnectionError(f"Unexpected error retrieving connection from MySQL pool: {e}")

    
    @contextmanager
    def get_connection_context(self):
        """
        Context manager for database connections.
        
        TODO: Implement context manager
        - Acquire connection from pool
        - Yield connection for use
        - Ensure connection is returned to pool
        - Handle exceptions gracefully
        """
        cnx = None
        cursor = None
        try:
            cnx = self.pool.get_connection()
            if not cnx.is_connected():
                self.logger.warning("Connection is not active, attempting to reconnect.")
                cnx.reconnect()
            yield cnx

        except Exception as e:  
            self.logger.error(f"Error during connection context: {e}")
            cnx.rollback()
            raise DatabaseOperationError(f"Error during database operation: {e}")
        
        except mysql.connector.Error as e:
            self.logger.error(f"MySQL error during connection context: {e}",
                              extra={
                                  'error_code': e.errno,
                                  'sql_state': getattr(e, 'sqlstate', None)
                              })
            cnx.rollback()
            raise DatabaseOperationError(f"MySQL error during database operation: {e}")
        
        else:
            cnx.commit()
        finally:
            # Ensure connection is returned to pool
            if cnx:
                try:
                    if cnx.is_connected():
                        cnx.close()
                        self.logger.info("Connection returned to pool.")
                    else:
                        self.logger.warning("Connection was not active, skipping return to pool.")

                except Exception as cleanup_error:
                    self.logger.error(f"Error closing connection: {cleanup_error}",
                                      extra={
                                          'error_code': getattr(cleanup_error, 'errno', None),
                                          'sql_state': getattr(cleanup_error, 'sqlstate', None)
                                      })
            # Ensure cursor is closed            
            if cursor:
                try:
                    cursor.close()
                    self.logger.info("Cursor closed successfully.")
                except:
                    pass


    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute SELECT query with parameters.
        
        Args:
            query: SQL query string
            params: Query parameters tuple
            
        Returns:
            List of dictionaries representing rows
        """
        with self.get_connection_context() as cnx:
            cursor = cnx.cursor(dictionary=True)
            try:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                self.logger.info(f"Query executed successfully: {query}")
                return rows
            
            except mysql.connector.Error as e:
                self.logger.error(f"Error executing query: {e}",
                                  extra={
                                      'error_code': e.errno,
                                      'sql_state': getattr(e, 'sqlstate', None),
                                      'query': query,
                                      'params': params
                                  })
                raise DatabaseOperationError(f"Error executing query: {e}")
            
            except Exception as e:
                self.logger.error(f"Unexpected error executing query: {e}",
                                  extra={
                                      'error_code': getattr(e, 'errno', None),
                                      'sql_state': getattr(e, 'sqlstate', None),
                                      'query': query,
                                      'params': params
                                  })
                raise DatabaseOperationError(f"Unexpected error executing query: {e}")
    
    def execute_update(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query string
            params: Query parameters tuple
            
        Returns:
            Number of affected rows
        
        """
        with self.get_connection_context() as cnx:
            cursor = cnx.cursor()
            try:
                cursor.execute(query, params)
                affected_rows = cursor.rowcount
                cnx.commit() 
                self.logger.info(f"Update executed successfully: {query}, affected rows: {affected_rows}")
                return affected_rows
            
            except mysql.connector.Error as e:
                self.logger.error(f"Error executing update: {e}",
                                  extra={
                                      'error_code': e.errno,
                                      'sql_state': getattr(e, 'sqlstate', None),
                                      'query': query,
                                      'params': params
                                  })
                cnx.rollback()
                raise DatabaseOperationError(f"Error executing update: {e}")
            
            except Exception as e:
                self.logger.error(f"Unexpected error executing update: {e}",
                                  extra={
                                      'error_code': getattr(e, 'errno', None),
                                      'sql_state': getattr(e, 'sqlstate', None),
                                      'query': query,
                                      'params': params
                                  })
                cnx.rollback()
                raise DatabaseOperationError(f"Unexpected error executing update: {e}")
    
    def execute_batch(self, query: str, params_list: List[Tuple]) -> int:
        """
        Execute batch operations for better performance.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            
        Returns:
            Total number of affected rows
        """
        total_affected = 0
        connection = None
        cursor = None

        try:
            connection = self.getconnection()
            cursor = connection.cursor()
            connection.start_transaction()  # Start transaction for batch operation
            cursor.executemany(query, params_list)
            total_affected = cursor.rowcount
            connection.commit()
            self.logger.info(f"Batch operation executed successfully: {query}, total affected rows: {total_affected}")
        
        except mysql.connector.Error as e:
            self.logger.error(f"Error executing batch operation: {e}",
                              extra={
                                  'error_code': e.errno,
                                  'sql_state': getattr(e, 'sqlstate', None),
                                  'query': query,
                                  'batch_size': len(params_list)
                              })
            if connection:
                connection.rollback()
            raise DatabaseOperationError(f"Error executing batch operation: {e}",
                                         context={'query': query, 'params_list': params_list})
        
        finally:
            if cursor:
                cursor.close()
            if connection:
                try:
                    connection.close()
                    self.logger.info("Connection returned to pool after batch operation.")
                except mysql.connector.Error as e:
                    self.logger.error(f"Error closing connection after batch operation: {e}",
                                      extra={
                                          'error_code': e.errno,
                                          'sql_state': getattr(e, 'sqlstate', None)
                                      })
        return total_affected
    
    @contextmanager
    def transaction(self):
        """
        Transaction context manager.
        
        TODO: Implement transaction management
        - Start transaction
        - Provide connection for transaction operations
        - Commit on success
        - Rollback on exceptions
        - Handle nested transactions
        """
        connection = None
        try:
            connection = self.getconnection()
            connection.start_transaction()
            yield connection  # Provide connection for transaction operations
            
            # Commit transaction if no exceptions
            connection.commit()
            self.logger.debug("Transaction committed successfully.", extra = {
                            'transaction_id': id(connection)})
        except Exception as e:
            if connection:
                try:
                    connection.rollback()
                    self.logger.error(f"Transaction rolled back due to error: {e}",
                                      extra={
                                          'error_code': getattr(e, 'errno', None),
                                          'sql_state': getattr(e, 'sqlstate', None),
                                          'transaction_id': id(connection)
                                      })
                except mysql.connector.Error as rollback_error:
                    self.logger.error(f"Error rolling back transaction: {rollback_error}",
                                      extra={
                                          'error_code': rollback_error.errno,
                                          'sql_state': getattr(rollback_error, 'sqlstate', None)
                                      })
            raise DatabaseOperationError(f"Transaction failed: {e}")
        finally:
            if connection:
                try:
                    connection.close()
                    self.logger.info("Connection returned to pool after transaction.",
                                     extra={'transaction_id': id(connection)})
                except mysql.connector.Error as e:
                    self.logger.error(f"Error closing connection after transaction: {e}",
                                      extra={
                                          'error_code': e.errno,
                                          'sql_state': getattr(e, 'sqlstate', None)
                                      })
    
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on database connection.
        
        Returns:
            Health status dictionary
            
        TODO: Implement health check
        - Test connection availability
        - Check pool status (active/idle connections)
        - Measure connection latency
        - Return comprehensive health status
        - Include performance metrics
        """
        connection = None
        try:
            connection = self.getconnection()
            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute("SHOW STATUS")
                # Fetching status information
                status = cursor.fetchall()
                self.logger.info("Database connection is healthy.")
                return status
            else:
                self.logger.warning("Database connection is not active.")
                return {'status': 'unhealthy', 'message': 'Connection is not active.'}
        except mysql.connector.Error as e:
            self.logger.error(f"Health check failed: {e}",
                              extra={
                                  'error_code': e.errno,
                                  'sql_state': getattr(e, 'sqlstate', None)
                              })
            return {'status': 'unhealthy', 'message': str(e)}
        finally:
            if connection:
                try:
                    connection.close()
                    self.logger.info("Connection closed after health check.")
                except mysql.connector.Error as e:
                    self.logger.error(f"Error closing connection after health check: {e}",
                                      extra={
                                          'error_code': e.errno,
                                          'sql_state': getattr(e, 'sqlstate', None)
                                      })
            if cursor:
                try:
                    cursor.close()
                    self.logger.info("Cursor closed after health check.")
                except mysql.connector.Error as e:
                    self.logger.error(f"Error closing cursor after health check: {e}",
                                      extra={
                                          'error_code': e.errno,
                                          'sql_state': getattr(e, 'sqlstate', None)
                                      })
    

    def close_pool(self):
        """
        Close all connections in the pool.
        
        TODO: Implement pool cleanup
        - Close all active connections
        - Clear connection pool
        - Log cleanup completion
        - Handle cleanup errors gracefully
        """
        try:
            if self.pool:
                self.pool.close()
                self.logger.info("MySQL connection pool closed successfully.")
        except mysql.connector.Error as e:
            self.logger.error(f"Error closing MySQL connection pool: {e}",
                              extra={
                                  'error_code': e.errno,
                                  'sql_state': getattr(e, 'sqlstate', None)
                              })
        except Exception as e:
            self.logger.error(f"Unexpected error closing MySQL connection pool: {e}",
                              extra={
                                  'error_code': getattr(e, 'errno', None),
                                  'sql_state': getattr(e, 'sqlstate', None)
                              })
            
    
    def get_pool_status(self) -> Dict[str, Any]:
        """
        Get current pool status and metrics.
        
        Returns:
            Pool status dictionary
            
        TODO: Implement pool monitoring
        - Return active connection count
        - Return idle connection count
        - Include pool configuration details
        - Add performance metrics
        """
        try:
            if not self.pool:
                self.logger.warning("Connection pool is not initialized.")
                return {'status': 'uninitialized', 
                        'message': 'Connection pool is not initialized.',
                        'active_connections': 0,
                        'idle_connections': 0
                    }
            
            idle_connections = self.pool._cnx_queue.qsize()
            total_connections = self.pool.pool_size
            active_connections = total_connections - idle_connections

            poolstatus = {
                'status': 'active',
                'message': 'Connection pool is active.',
                'active_connections': active_connections,
                'idle_connections': idle_connections,
                'pool_size': total_connections,
                'pool_name': self.pool.pool_name,
                'utilization_percent': (active_connections / total_connections) * 100,
                'config': {
                    'host': self.config['default']['host'],
                    'port': self.config['default']['port'],
                    'user': self.config['default']['user'],
                    'database': self.config['default']['database'],
                    'pool_size': self.config['default']['pool_size']
                } 
            }
            return poolstatus
            
        except Exception as e:
            self.logger.error(f"Error getting pool status: {e}",
                              extra={
                                  'error_code': getattr(e, 'errno', None),
                                  'sql_state': getattr(e, 'sqlstate', None)
                              })
            status = {
                'status': 'error',
                'message': str(e),
                'active_connections': 0,
                'idle_connections': 0
            }
            return status
        