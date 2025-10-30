import logging
from datetime import datetime

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, model_validator


class AWSParameters(BaseModel):
    """AWS credentials and configuration."""

    api_key_id: str
    api_key_secret: str = Field(alias="#api_key_secret")
    s3_bucket: str
    aws_region: str = Field(default="eu-central-1")


class LoadingOptions(BaseModel):
    """Data loading configuration."""

    incremental_output: int = Field(default=0, ge=0, le=1)  # 0=Full Load, 1=Incremental
    pkey: list[str] = Field(default=[])  # Primary key columns

    @property
    def incremental_output_bool(self) -> bool:
        return self.incremental_output == 1


class Configuration(BaseModel):
    """Main component configuration."""

    aws_parameters: AWSParameters
    report_path_prefix: str
    min_date_since: str | None = None
    max_date: str = "now"
    since_last: bool = True
    loading_options: LoadingOptions = Field(default_factory=LoadingOptions)
    debug: bool = False

    @property
    def start_datetime(self) -> datetime:
        """Get start date as datetime object."""
        since = self.min_date_since or "2000-01-01"
        return datetime.strptime(since, "%Y-%m-%d")

    @property
    def end_datetime(self) -> datetime:
        """Get end date as datetime object."""
        if self.max_date == "now":
            return datetime.now()
        return datetime.strptime(self.max_date, "%Y-%m-%d")

    @model_validator(mode="after")
    def validate_dates(self) -> "Configuration":
        """Validate date formats."""
        # fix when old config had empty max_date
        if self.max_date == "":
            self.max_date = "now"
        
        # Validate min_date_since
        if self.min_date_since is not None and self.min_date_since != "":
            try:
                datetime.strptime(self.min_date_since, "%Y-%m-%d")
            except ValueError:
                raise ValueError("min_date_since must be in YYYY-MM-DD format")

        # Validate max_date
        if self.max_date != "now":
            try:
                datetime.strptime(self.max_date, "%Y-%m-%d")
            except ValueError:
                raise ValueError("max_date must be 'now' or YYYY-MM-DD format")

        return self

    def __init__(self, /, **data):
        try:
            super().__init__(**data)
            if self.debug:
                logging.debug("Component will run in Debug mode")
        except ValidationError as e:
            error_messages = []
            for err in e.errors():
                # Handle Pydantic V2 error format properly
                if "loc" in err and err["loc"]:
                    location = ".".join(str(x) for x in err["loc"])
                else:
                    location = "unknown"
                error_messages.append(f"{location}: {err.get('msg', 'Validation error')}")
            raise UserException(f"Configuration validation error: {', '.join(error_messages)}")
