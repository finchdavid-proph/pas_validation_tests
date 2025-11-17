from validation_pk.graph.iterator_validation_keep.config.Config import SubgraphConfig as iterator_validation_keep_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, iterator_validation_keep: dict=None, prophecy_project_config=None, **kwargs):
        self.spark = None
        self.update(iterator_validation_keep, prophecy_project_config, **kwargs)

    def update(self, iterator_validation_keep: dict={}, prophecy_project_config=None, **kwargs):
        prophecy_spark = self.spark
        prophecy_project_config = self.update_project_conf_values(prophecy_project_config, kwargs)
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config)
        self.iterator_validation_keep = self.get_config_object(
            prophecy_spark, 
            iterator_validation_keep_Config(
              prophecy_spark = prophecy_spark, 
              prophecy_project_config = prophecy_project_config
            ), 
            iterator_validation_keep, 
            iterator_validation_keep_Config, 
            prophecy_project_config
        )
        pass
