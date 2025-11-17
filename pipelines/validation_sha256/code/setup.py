from setuptools import setup, find_packages
setup(
    name = 'validation_sha256',
    version = '1.0',
    packages = find_packages(include = ('validation_sha256*', )) + ['prophecy_config_instances.validation_sha256'],
    package_dir = {'prophecy_config_instances.validation_sha256' : 'configs/resources/validation_sha256'},
    package_data = {'prophecy_config_instances.validation_sha256' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.4'],
    entry_points = {
'console_scripts' : [
'main = validation_sha256.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
