"""
Configuration Manager for Market Data Pipeline

This module provides functionality to load, validate, and access
configuration settings from YAML files.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class ConfigManager:
    """
    Manages configuration for the market data pipeline.
    
    Loads and validates YAML configuration files and provides
    convenient access to settings.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the ConfigManager.
        
        Args:
            config_dir: Path to configuration directory. 
                       Defaults to 'config' in project root.
        """
        self.logger = logging.getLogger(__name__)
        
        # Determine config directory
        if config_dir is None:
            # Assume config is in project root / config
            project_root = Path(__file__).parent.parent
            self.config_dir = project_root / "config"
        else:
            self.config_dir = Path(config_dir)
        
        # Configuration storage
        self.config: Dict[str, Any] = {}
        self.stocks: List[Dict[str, str]] = []
        self.indices: List[Dict[str, str]] = []
        
        self.logger.info(f"ConfigManager initialized with config_dir: {self.config_dir}")
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load all configuration files (config.yaml, stocks.yaml, indices.yaml).
        
        Returns:
            Dictionary containing all configuration data.
            
        Raises:
            ConfigurationError: If configuration files cannot be loaded or validated.
        """
        try:
            # Load main configuration
            config_file = self.config_dir / "config.yaml"
            self.config = self._load_yaml_file(config_file)
            self.logger.info("Main configuration loaded successfully")
            
            # Load stocks configuration
            stocks_file = self.config_dir / "stocks.yaml"
            stocks_data = self._load_yaml_file(stocks_file)
            self.stocks = stocks_data.get("stocks", [])
            self.logger.info(f"Loaded {len(self.stocks)} stock symbols")
            
            # Load indices configuration
            indices_file = self.config_dir / "indices.yaml"
            indices_data = self._load_yaml_file(indices_file)
            self.indices = indices_data.get("indices", [])
            self.logger.info(f"Loaded {len(self.indices)} index symbols")
            
            # Validate configuration
            self.validate_config()
            
            return {
                "config": self.config,
                "stocks": self.stocks,
                "indices": self.indices
            }
            
        except Exception as e:
            error_msg = f"Failed to load configuration: {str(e)}"
            self.logger.error(error_msg)
            raise ConfigurationError(error_msg) from e
    
    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Load a YAML file and return its contents.
        
        Args:
            file_path: Path to the YAML file.
            
        Returns:
            Dictionary containing the YAML data.
            
        Raises:
            ConfigurationError: If file doesn't exist or cannot be parsed.
        """
        if not file_path.exists():
            raise ConfigurationError(f"Configuration file not found: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                
            if data is None:
                raise ConfigurationError(f"Empty configuration file: {file_path}")
                
            self.logger.debug(f"Loaded YAML file: {file_path}")
            return data
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {file_path}: {str(e)}") from e
        except Exception as e:
            raise ConfigurationError(f"Error reading {file_path}: {str(e)}") from e
    
    def validate_config(self) -> bool:
        """
        Validate the loaded configuration.
        
        Returns:
            True if configuration is valid.
            
        Raises:
            ConfigurationError: If configuration is invalid.
        """
        self.logger.info("Validating configuration...")
        
        # Validate main config
        required_main_fields = ['data_dir', 'log_dir', 'intervals', 'max_retries', 
                                'retry_delay', 'chunk_size', 'validate_data', 'timezone']
        
        for field in required_main_fields:
            if field not in self.config:
                raise ConfigurationError(f"Missing required field in config.yaml: {field}")
        
        # Validate data types
        if not isinstance(self.config['intervals'], list):
            raise ConfigurationError("'intervals' must be a list")
        
        if not isinstance(self.config['max_retries'], int) or self.config['max_retries'] < 0:
            raise ConfigurationError("'max_retries' must be a non-negative integer")
        
        if not isinstance(self.config['retry_delay'], (int, float)) or self.config['retry_delay'] < 0:
            raise ConfigurationError("'retry_delay' must be a non-negative number")
        
        if not isinstance(self.config['chunk_size'], int) or self.config['chunk_size'] <= 0:
            raise ConfigurationError("'chunk_size' must be a positive integer")
        
        if not isinstance(self.config['validate_data'], bool):
            raise ConfigurationError("'validate_data' must be a boolean")
        
        # Validate intervals
        valid_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']
        for interval in self.config['intervals']:
            if interval not in valid_intervals:
                self.logger.warning(f"Interval '{interval}' may not be supported by yfinance")
        
        # Validate stocks
        if not self.stocks:
            self.logger.warning("No stocks configured in stocks.yaml")
        else:
            for idx, stock in enumerate(self.stocks):
                if 'symbol' not in stock:
                    raise ConfigurationError(f"Stock at index {idx} missing 'symbol' field")
        
        # Validate indices
        if not self.indices:
            self.logger.warning("No indices configured in indices.yaml")
        else:
            for idx, index in enumerate(self.indices):
                if 'symbol' not in index:
                    raise ConfigurationError(f"Index at index {idx} missing 'symbol' field")
        
        self.logger.info("Configuration validation successful")
        return True
    
    def get_stock_list(self) -> List[str]:
        """
        Get list of stock symbols.
        
        Returns:
            List of stock symbols (e.g., ['RELIANCE.NS', 'TCS.NS', ...])
        """
        if not self.stocks:
            self.logger.warning("Stock list is empty. Have you called load_config()?")
        
        return [stock['symbol'] for stock in self.stocks if 'symbol' in stock]
    
    def get_indices_list(self) -> List[str]:
        """
        Get list of index symbols.
        
        Returns:
            List of index symbols (e.g., ['^NSEI', '^NSEBANK', ...])
        """
        if not self.indices:
            self.logger.warning("Indices list is empty. Have you called load_config()?")
        
        return [index['symbol'] for index in self.indices if 'symbol' in index]
    
    def get_intervals(self) -> List[str]:
        """
        Get list of configured data intervals.
        
        Returns:
            List of interval strings (e.g., ['1m', '5m', '1d'])
        """
        return self.config.get('intervals', [])
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Get a specific configuration value.
        
        Args:
            key: Configuration key to retrieve.
            default: Default value if key doesn't exist.
            
        Returns:
            Configuration value or default.
        """
        return self.config.get(key, default)
    
    def get_data_dir(self) -> str:
        """Get the data directory path."""
        return self.config.get('data_dir', './data')
    
    def get_log_dir(self) -> str:
        """Get the log directory path."""
        return self.config.get('log_dir', './logs')
    
    def get_timezone(self) -> str:
        """Get the configured timezone."""
        return self.config.get('timezone', 'Asia/Kolkata')
    
    def get_max_retries(self) -> int:
        """Get the maximum number of retries for API calls."""
        return self.config.get('max_retries', 3)
    
    def get_retry_delay(self) -> float:
        """Get the delay between retries in seconds."""
        return self.config.get('retry_delay', 5)
    
    def get_chunk_size(self) -> int:
        """Get the chunk size for batch processing."""
        return self.config.get('chunk_size', 10)
    
    def should_validate_data(self) -> bool:
        """Check if data validation is enabled."""
        return self.config.get('validate_data', True)
    
    def get_all_symbols(self) -> List[str]:
        """
        Get combined list of all stock and index symbols.
        
        Returns:
            List of all symbols (stocks + indices)
        """
        return self.get_stock_list() + self.get_indices_list()
    
    def get_stocks_by_sector(self, sector: str) -> List[Dict[str, str]]:
        """
        Get stocks filtered by sector (if sector information is available).
        
        Args:
            sector: Sector name to filter by.
            
        Returns:
            List of stock dictionaries matching the sector.
        """
        return [stock for stock in self.stocks 
                if stock.get('sector', '').lower() == sector.lower()]
    
    def __repr__(self) -> str:
        """String representation of ConfigManager."""
        return (f"ConfigManager(config_dir='{self.config_dir}', "
                f"stocks={len(self.stocks)}, indices={len(self.indices)})")


# Singleton instance for easy access
_config_manager_instance: Optional[ConfigManager] = None


def get_config_manager(config_dir: Optional[str] = None) -> ConfigManager:
    """
    Get or create a ConfigManager singleton instance.
    
    Args:
        config_dir: Path to configuration directory (only used on first call).
        
    Returns:
        ConfigManager instance.
    """
    global _config_manager_instance
    
    if _config_manager_instance is None:
        _config_manager_instance = ConfigManager(config_dir)
        _config_manager_instance.load_config()
    
    return _config_manager_instance


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and use config manager
    try:
        cm = ConfigManager()
        cm.load_config()
        
        print(f"\n{cm}")
        print(f"\nData Directory: {cm.get_data_dir()}")
        print(f"Log Directory: {cm.get_log_dir()}")
        print(f"Timezone: {cm.get_timezone()}")
        print(f"Intervals: {cm.get_intervals()}")
        print(f"Max Retries: {cm.get_max_retries()}")
        print(f"Chunk Size: {cm.get_chunk_size()}")
        print(f"\nTotal Stocks: {len(cm.get_stock_list())}")
        print(f"Total Indices: {len(cm.get_indices_list())}")
        print(f"\nFirst 5 stocks: {cm.get_stock_list()[:5]}")
        print(f"First 5 indices: {cm.get_indices_list()[:5]}")
        
    except ConfigurationError as e:
        print(f"Configuration Error: {e}")
