from setuptools import setup, find_packages
setup(
    name = 'validation_mask',
    version = '1.0',
    packages = find_packages(include = ('validation_mask*', )) + ['prophecy_config_instances.validation_mask'],
    package_dir = {'prophecy_config_instances.validation_mask' : 'configs/resources/validation_mask'},
    package_data = {'prophecy_config_instances.validation_mask' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.4'],
    entry_points = {
'console_scripts' : [
'main = validation_mask.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
