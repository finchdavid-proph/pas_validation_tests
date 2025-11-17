from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(
            self,
            prophecy_spark=None,
            validation: str="",
            source_catalog: str="",
            source_schema: str="",
            source_table: str="",
            column: str="",
            rule_type: str="",
            logic: str="",
            prophecy_project_config=None,
            **kwargs
    ):
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config, kwargs)
        self.validation = validation
        self.source_catalog = source_catalog
        self.source_schema = source_schema
        self.source_table = source_table
        self.column = column
        self.rule_type = rule_type
        self.logic = logic
        pass

    def update(self, updated_config):
        self.validation = updated_config.validation
        self.source_catalog = updated_config.source_catalog
        self.source_schema = updated_config.source_schema
        self.source_table = updated_config.source_table
        self.column = updated_config.column
        self.rule_type = updated_config.rule_type
        self.logic = updated_config.logic
        self.update_project_config_fields(updated_config)
        pass

Config = SubgraphConfig()
