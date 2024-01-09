from setuptools import setup, find_packages

setup(
    name="bloom_lims",
    version="0.7.8",
    packages=find_packages(),
    install_requires=[
        # Add dependencies here,
        # 'pytest',
    ],
    entry_points={
        "console_scripts": [
            "install-bloom-lims=bloom_lims.install_couchdb:main",
        ],
    },
)
