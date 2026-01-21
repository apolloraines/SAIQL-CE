from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="saiql",
    version="0.3.0-alpha",
    author="Apollo Raines",
    author_email="apollo@saiql.ai",
    description="SAIQL: Semantic Artificial Intelligence Query Language",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apolloraines/SAIQL",
    packages=find_packages(),
    py_modules=["saiql"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "saiql=shell.query_shell:main",
            "saiql-server=saiql_production_server:main",
        ],
    },
    include_package_data=True,
)
