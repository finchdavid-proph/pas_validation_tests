from setuptools import setup, find_packages
setup(
    name = 'validation_supress',
    version = '1.0',
    packages = find_packages(include = ('validation_suppress*', )) + ['prophecy_config_instances.validation_suppress'],
    package_dir = {'prophecy_config_instances.validation_suppress' : 'configs/resources/validation_suppress'},
    package_data = {'prophecy_config_instances.validation_suppress' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.4'],
    entry_points = {
'console_scripts' : [
'main = validation_suppress.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
