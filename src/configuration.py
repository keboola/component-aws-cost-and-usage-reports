import logging
from typing import Optional, List
from pydantic import BaseModel, Field, ValidationError
from keboola.component.exceptions import UserException


class AWSParameters(BaseModel):
    """AWS credentials and configuration."""

    api_key_id: str
    api_key_secret: str = Field(alias="#api_key_secret")
    s3_bucket: str
    aws_region: str = Field(default="eu-central-1")


class LoadingOptions(BaseModel):
    """Data loading configuration."""

    incremental_output: int = Field(default=0, ge=0, le=1)  # 0=Full Load, 1=Incremental
    pkey: List[str] = Field(default=[])  # Primary key columns

    @property
    def incremental_output_bool(self) -> bool:
        return self.incremental_output == 1


class Configuration(BaseModel):
    """Main component configuration."""

    aws_parameters: AWSParameters
    report_path_prefix: str
    min_date_since: Optional[str] = None
    max_date: str = "now"
    since_last: bool = True
    loading_options: LoadingOptions = Field(default_factory=LoadingOptions)
    debug: bool = False

    def __init__(self, /, **data):
        try:
            super().__init__(**data)
            if self.debug:
                logging.debug("Component will run in Debug mode")
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(
                f"Configuration validation error: {', '.join(error_messages)}"
            )
