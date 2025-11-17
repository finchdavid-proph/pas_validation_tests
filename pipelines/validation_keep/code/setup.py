from setuptools import setup, find_packages
setup(
    name = 'validation_keep',
    version = '1.0',
    packages = find_packages(include = ('validation_keep*', )) + ['prophecy_config_instances.validation_keep'],
    package_dir = {'prophecy_config_instances.validation_keep' : 'configs/resources/validation_keep'},
    package_data = {'prophecy_config_instances.validation_keep' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.4'],
    entry_points = {
'console_scripts' : [
'main = validation_keep.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
