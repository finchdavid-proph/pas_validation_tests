from setuptools import setup, find_packages
setup(
    name = 'validation_pk',
    version = '1.0',
    packages = find_packages(include = ('validation_pk*', )) + ['prophecy_config_instances.validation_pk'],
    package_dir = {'prophecy_config_instances.validation_pk' : 'configs/resources/validation_pk'},
    package_data = {'prophecy_config_instances.validation_pk' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.4'],
    entry_points = {
'console_scripts' : [
'main = validation_pk.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
