"""Application configuration via pydantic-settings.

All settings are read from environment variables with sensible defaults.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- Service ---
    service_name: str = "arcforge-daytona-service"
    service_version: str = "0.1.0"
    debug: bool = False
    log_level: str = "info"
    host: str = "0.0.0.0"
    port: int = 8000

    # --- Daytona ---
    daytona_api_key: str = ""
    # Target region for sandbox creation. The org default ("eu") has been
    # landing every sandbox in state=Error with no error_reason — "us" is
    # the reliable default. Override with DAYTONA_TARGET env var.
    daytona_target: str = "us"
    daytona_default_timeout: int = 300  # seconds for sandbox provisioning
    daytona_max_concurrent_sandboxes: int = 50

    # --- Sandbox defaults ---
    default_sandbox_language: str = "python"
    default_sandbox_image: str = "daytonaio/workspace-python:latest"
    default_cpu: float = 2.0
    default_memory: str = "4Gi"
    default_disk: str = "10Gi"
    sandbox_idle_timeout_seconds: int = 1800  # 30 minutes

    # --- Module 3 Browser Engine (Playwright-in-VM) ---
    browser_install_timeout_s: int = 300   # max time to install Chromium (~150MB download + deps)
    browser_audit_timeout_s: int = 120     # max time for one audit run (launch + goto + screenshot)
    browser_audit_script_path: str = "/workspace/.browser-audit.py"

    @property
    def daytona_config_dict(self) -> dict:
        cfg = {
            "api_key": self.daytona_api_key,
        }
        if self.daytona_target:
            cfg["target"] = self.daytona_target
        return cfg


settings = Settings()
