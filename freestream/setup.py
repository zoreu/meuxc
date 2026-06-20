from setuptools import setup, find_packages

setup(
    name="freestream",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "flask",
        "requests"
    ],
    entry_points={
    "console_scripts": [
        "freestream=freestream.xc_server:run_service"
    ]
}
)