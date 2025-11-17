from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            bronze_catalog: str="",
            bronze_schema: str="",
            bronze_table: str="",
            bronze_path: str="",
            silver_catalog: str="",
            silver_schema: str="",
            silver_table: str="",
            silver_path: str="",
            suppress_columns: list=[],
            keep: bool=True,
            supress: bool=True,
            map: bool=True,
            sha256: bool=True,
            mask: bool=True,
            prophecy_project_config=None,
            **kwargs
    ):
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config, kwargs)
        self.bronze_catalog = bronze_catalog
        self.bronze_schema = bronze_schema
        self.bronze_table = bronze_table
        self.bronze_path = bronze_path
        self.silver_catalog = silver_catalog
        self.silver_schema = silver_schema
        self.silver_table = silver_table
        self.silver_path = silver_path
        self.suppress_columns = suppress_columns
        self.keep = keep
        self.supress = supress
        self.map = map
        self.sha256 = sha256
        self.mask = mask
        pass

    def update(self, updated_config):
        self.bronze_catalog = updated_config.bronze_catalog
        self.bronze_schema = updated_config.bronze_schema
        self.bronze_table = updated_config.bronze_table
        self.bronze_path = updated_config.bronze_path
        self.silver_catalog = updated_config.silver_catalog
        self.silver_schema = updated_config.silver_schema
        self.silver_table = updated_config.silver_table
        self.silver_path = updated_config.silver_path
        self.suppress_columns = updated_config.suppress_columns
        self.keep = updated_config.keep
        self.supress = updated_config.supress
        self.map = updated_config.map
        self.sha256 = updated_config.sha256
        self.mask = updated_config.mask
        self.update_project_config_fields(updated_config)
        pass

Config = SubgraphConfig()
