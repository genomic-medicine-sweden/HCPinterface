
from pytest import Config, fixture, UsageError
from configparser import ConfigParser
from typing import Any

from NGPIris.hcp import HCPHandler

class CustomConfig:
    """A typed wrapper around pytest.Config for dynamic attributes."""
    def __init__(self, pytest_config : Config):
        self._config = pytest_config

    @property
    def hcp_h(self) -> HCPHandler:
        """Access the HCPHandler instance."""
        return getattr(self._config, "hcp_h")

    def __getattr__(self, name : str) -> Any:
        """Dynamically get attributes set during pytest configuration."""
        return getattr(self._config, name)

def set_section(config : Config, parser : ConfigParser, section : str) -> None:
    parse_dict = dict(parser.items(section))
    for k, v in parse_dict.items():
        setattr(config, k, v)  # Adds attributes dynamically to pytest.Config

def pytest_addoption(parser) -> None:
    parser.addoption(
        "--config",
        action="store",
        default=None,
        help="Path to the configuration file (e.g., path/to/config.ini)",
    )

def pytest_configure(config : Config) -> None:
    config_path = config.getoption("--config")
    if not config_path:
        raise UsageError("--config argument is required.")
    else:
        parser = ConfigParser()
        parser.read(str(config_path))

        # Dynamically add an HCPHandler instance to config
        setattr(config, "hcp_h", HCPHandler(parser.get("General", "credentials_path")))

        # Dynamically add all key-value pairs from "HCP_tests" section
        set_section(config, parser, "HCP_tests")


@fixture
def custom_config(pytestconfig : Config) -> CustomConfig:
    """Provide the typed wrapper for pytest.Config."""
    return CustomConfig(pytestconfig)
