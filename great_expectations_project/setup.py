from setuptools import setup, find_packages

setup(
    name='great_expectations_project',
    version='0.1.0',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        'great_expectations',
        'sqlalchemy',
        'psycopg2-binary',
    ],
    entry_points={
        'console_scripts': [
            'run-validations=scripts.run_validations:main',
        ],
    },
)
