from validation_map.graph.TableIterator_1.config.Config import SubgraphConfig as TableIterator_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, TableIterator_1: dict=None, prophecy_project_config=None, **kwargs):
        self.spark = None
        self.update(TableIterator_1, prophecy_project_config, **kwargs)

    def update(self, TableIterator_1: dict={}, prophecy_project_config=None, **kwargs):
        prophecy_spark = self.spark
        prophecy_project_config = self.update_project_conf_values(prophecy_project_config, kwargs)
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config)
        self.TableIterator_1 = self.get_config_object(
            prophecy_spark, 
            TableIterator_1_Config(prophecy_spark = prophecy_spark, prophecy_project_config = prophecy_project_config), 
            TableIterator_1, 
            TableIterator_1_Config, 
            prophecy_project_config
        )
        pass
