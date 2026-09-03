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
    # Optional target region override. The org only has access to 'eu' —
    # forcing an unavailable region fails creation with 'Region <x> is not
    # available to the organization'. Leave empty for the org default.
    daytona_target: str = ""
    daytona_default_timeout: int = 300  # seconds for sandbox provisioning
    daytona_max_concurrent_sandboxes: int = 50

    # --- Sandbox defaults ---
    default_sandbox_language: str = "python"
    default_sandbox_image: str = "daytonaio/workspace-python:latest"
    # Classic snapshot used for project workspaces. The SDK's
    # language=code-toolbox path is broken in the eu region (every sandbox
    # lands in state=Error) — classic snapshots boot reliably. daytona-medium
    # carries cpu=2 / mem=4Gi / disk=8Gi and ships python3 + node + bash/zsh +
    # passwordless sudo (all verified live 2026-08-25).
    default_workspace_snapshot: str = "daytona-medium"
    default_cpu: float = 2.0
    default_memory: str = "4Gi"
    default_disk: str = "10Gi"
    sandbox_idle_timeout_seconds: int = 1800  # 30 minutes

    # --- Quota hygiene (incident 2026-08-27: "sandbox is full, again") ---
    # The quota reaper's periodic schedule MUST be LONGER than
    # sandbox_idle_timeout_seconds: every Daytona list/read refreshes
    # lastActivityAt for the listed sandboxes (asynchronously), so a reaper
    # that lists more often than the idle threshold would keep every
    # sandbox looking fresh and never reap anything (self-poisoning).
    reaper_interval_seconds: int = 2100  # 35 minutes > 1800s idle timeout
    reaper_first_run_delay_seconds: int = 90
    # Absolute lifetime cap for workspace sandboxes regardless of activity
    # (immune to lastActivityAt poisoning). 0 disables the cap.
    sandbox_max_lifetime_seconds: int = 43200  # 12 hours
    # Absolute lifetime cap for type=probe sandboxes (disposable engine /
    # health probes — incident 2026-09-02: one leaked "engine-probe"
    # sandbox held 4 GiB for 21+ hours). Creation-time based, refresh-proof.
    probe_max_lifetime_seconds: int = 3600  # 1 hour
    # How many oldest sandboxes the emergency force-free path may delete
    # when a quota-blocked creation must not fail. The requester's own
    # sandboxes created within force_free_protect_recent_seconds are
    # shielded (in-flight builds); older ones are freed like any corpse.
    quota_force_free_max: int = 3
    force_free_protect_recent_seconds: int = 1800  # 30 minutes

    # --- Render free-tier keep-alive (incident 2026-09-03: Uptime Robot
    # "can't be reached" — free services spin down after 15 min idle and the
    # cold start outlasts monitor timeouts). Self-pings the public URL.
    keepalive_enabled: bool = True
    keepalive_interval_seconds: int = 600  # 10 min < Render's 15 min idle cutoff
    keepalive_path: str = "/health"  # static 200, zero side effects

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
