from setuptools import setup, find_packages
setup(
    name = 'validation_map_2',
    version = '1.0',
    packages = find_packages(include = ('validation_map_2*', )) + ['prophecy_config_instances.validation_map_2'],
    package_dir = {'prophecy_config_instances.validation_map_2' : 'configs/resources/validation_map_2'},
    package_data = {'prophecy_config_instances.validation_map_2' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.4'],
    entry_points = {
'console_scripts' : [
'main = validation_map_2.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
