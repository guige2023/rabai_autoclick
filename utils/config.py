"""Configuration management utilities for RabAI AutoClick.

Provides:
- Configuration loading
- Configuration validation
- Environment-based configuration
"""

import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class ConfigOption:
    """Configuration option definition."""
    name: str
    value: Any
    default: Any = None
    description: str = ""
    validator: Optional[Callable[[Any], bool]] = None
    required: bool = False


class Config:
    """Configuration container."""

    def __init__(self) -> None:
        """Initialize configuration."""
        self._options: Dict[str, ConfigOption] = {}
        self._raw: Dict[str, Any] = {}

    def add_option(
        self,
        name: str,
        default: Any = None,
        description: str = "",
        validator: Optional[Callable[[Any], bool]] = None,
        required: bool = False,
    ) -> "Config":
        """Add a configuration option.

        Args:
            name: Option name.
            default: Default value.
            description: Option description.
            validator: Validation function.
            required: Whether option is required.

        Returns:
            Self for chaining.
        """
        self._options[name] = ConfigOption(
            name=name,
            value=default,
            default=default,
            description=description,
            validator=validator,
            required=required,
        )
        return self

    def get(self, name: str, default: Any = None) -> Any:
        """Get configuration value.

        Args:
            name: Option name.
            default: Default if not found.

        Returns:
            Configuration value.
        """
        if name in self._options:
            return self._options[name].value
        return default

    def set(self, name: str, value: Any) -> bool:
        """Set configuration value.

        Args:
            name: Option name.
            value: Value to set.

        Returns:
            True if set successfully.
        """
        if name not in self._options:
            # Auto-add option
            self._options[name] = ConfigOption(name=name, value=value)
            return True

        option = self._options[name]
        if option.validator and not option.validator(value):
            return False

        option.value = value
        return True

    def validate(self) -> List[str]:
        """Validate configuration.

        Returns:
            List of validation errors.
        """
        errors = []

        for name, option in self._options.items():
            if option.required and option.value is None:
                errors.append(f"Required option '{name}' is not set")

            if option.validator and option.value is not None:
                try:
                    if not option.validator(option.value):
                        errors.append(f"Option '{name}' validation failed")
                except Exception as e:
                    errors.append(f"Option '{name}' validation error: {e}")

        return errors

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Configuration as dict.
        """
        return {name: opt.value for name, opt in self._options.items()}

    def from_dict(self, data: Dict[str, Any]) -> None:
        """Load from dictionary.

        Args:
            data: Configuration data.
        """
        for name, value in data.items():
            self.set(name, value)


class EnvironmentConfig(Config):
    """Configuration with environment variable support."""

    def __init__(self, prefix: str = "RABAI_") -> None:
        """Initialize environment config.

        Args:
            prefix: Environment variable prefix.
        """
        super().__init__()
        self._prefix = prefix

    def load_from_env(self) -> None:
        """Load configuration from environment variables."""
        for name, option in self._options.items():
            env_var = f"{self._prefix}{name.upper()}"
            env_value = os.environ.get(env_var)

            if env_value is not None:
                # Type conversion based on default
                if isinstance(option.default, bool):
                    option.value = env_value.lower() in ("true", "1", "yes")
                elif isinstance(option.default, int):
                    try:
                        option.value = int(env_value)
                    except ValueError:
                        pass
                elif isinstance(option.default, float):
                    try:
                        option.value = float(env_value)
                    except ValueError:
                        pass
                else:
                    option.value = env_value


class ConfigLoader:
    """Load configuration from files."""

    @staticmethod
    def load_json(path: str) -> Optional[Dict[str, Any]]:
        """Load JSON configuration.

        Args:
            path: Path to JSON file.

        Returns:
            Configuration dict or None.
        """
        import json
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return None

    @staticmethod
    def load_yaml(path: str) -> Optional[Dict[str, Any]]:
        """Load YAML configuration.

        Args:
            path: Path to YAML file.

        Returns:
            Configuration dict or None.
        """
        try:
            import yaml
            with open(path, "r") as f:
                return yaml.safe_load(f)
        except Exception:
            return None

    @staticmethod
    def save_json(path: str, config: Dict[str, Any]) -> bool:
        """Save JSON configuration.

        Args:
            path: Output path.
            config: Configuration to save.

        Returns:
            True if successful.
        """
        import json
        try:
            with open(path, "w") as f:
                json.dump(config, f, indent=2)
            return True
        except Exception:
            return False

    @staticmethod
    def load_env_file(path: str) -> None:
        """Load .env file into environment.

        Args:
            path: Path to .env file.
        """
        try:
            from dotenv import load_dotenv
            load_dotenv(path)
        except Exception:
            pass


class ConfigValidator:
    """Validate configuration values."""

    @staticmethod
    def is_positive_int(value: Any) -> bool:
        """Check if value is positive integer."""
        return isinstance(value, int) and value > 0

    @staticmethod
    def is_non_negative_int(value: Any) -> bool:
        """Check if value is non-negative integer."""
        return isinstance(value, int) and value >= 0

    @staticmethod
    def is_in_range(min_val: float, max_val: float) -> Callable[[Any], bool]:
        """Create range validator.

        Args:
            min_val: Minimum value.
            max_val: Maximum value.

        Returns:
            Validator function.
        """
        def validator(value: Any) -> bool:
            try:
                return min_val <= float(value) <= max_val
            except (ValueError, TypeError):
                return False
        return validator

    @staticmethod
    def is_one_of(choices: List[Any]) -> Callable[[Any], bool]:
        """Create choices validator.

        Args:
            choices: Valid choices.

        Returns:
            Validator function.
        """
        def validator(value: Any) -> bool:
            return value in choices
        return validator

    @staticmethod
    def is_path() -> Callable[[Any], bool]:
        """Create path validator.

        Returns:
            Validator function.
        """
        def validator(value: Any) -> bool:
            return isinstance(value, str) and len(value) > 0
        return validator


class ConfigSection:
    """Group related configuration options."""

    def __init__(self, name: str) -> None:
        """Initialize section.

        Args:
            name: Section name.
        """
        self.name = name
        self._config = Config()

    def add_option(
        self,
        name: str,
        default: Any = None,
        **kwargs,
    ) -> "ConfigSection":
        """Add option to section.

        Args:
            name: Option name.
            default: Default value.
            **kwargs: Additional arguments.

        Returns:
            Self for chaining.
        """
        self._config.add_option(name, default, **kwargs)
        return self

    def get(self, name: str, default: Any = None) -> Any:
        """Get option value."""
        return self._config.get(name, default)

    def set(self, name: str, value: Any) -> bool:
        """Set option value."""
        return self._config.set(name, value)

    def validate(self) -> List[str]:
        """Validate all options."""
        return self._config.validate()

    def to_dict(self) -> Dict[str, Any]:
        """Get all options as dict."""
        return self._config.to_dict()


class ConfigManager:
    """Manage multiple configuration sections."""

    def __init__(self) -> None:
        """Initialize manager."""
        self._sections: Dict[str, ConfigSection] = {}

    def add_section(self, name: str) -> ConfigSection:
        """Add a configuration section.

        Args:
            name: Section name.

        Returns:
            Created section.
        """
        section = ConfigSection(name)
        self._sections[name] = section
        return section

    def get_section(self, name: str) -> Optional[ConfigSection]:
        """Get section by name.

        Args:
            name: Section name.

        Returns:
            Section or None.
        """
        return self._sections.get(name)

    def validate_all(self) -> List[str]:
        """Validate all sections.

        Returns:
            List of validation errors.
        """
        errors = []
        for name, section in self._sections.items():
            section_errors = section.validate()
            for error in section_errors:
                errors.append(f"[{name}] {error}")
        return errors

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """Get all sections as dict.

        Returns:
            All sections and options.
        """
        return {name: section.to_dict() for name, section in self._sections.items()}
