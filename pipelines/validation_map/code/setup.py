from setuptools import setup, find_packages
setup(
    name = 'validation_map',
    version = '1.0',
    packages = find_packages(include = ('validation_map*', )) + ['prophecy_config_instances.validation_map'],
    package_dir = {'prophecy_config_instances.validation_map' : 'configs/resources/validation_map'},
    package_data = {'prophecy_config_instances.validation_map' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.4'],
    entry_points = {
'console_scripts' : [
'main = validation_map.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
