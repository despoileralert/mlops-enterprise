from setuptools import setup, find_packages

setup(
    name="mlops_enterprise",
    version="0.1.0",
    description="Enterprise MLOps Platform",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="euleong",
    author_email="euleong@hotmail.com",
    url="https://github.com/yourusername/mlops-enterprise",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "numpy>=1.21.0",
        "pandas>=1.3.0",
        "scikit-learn>=1.0.0",
        "pyspark>=3.3.0",
        "mysql-connector-python>=8.0.0",
        "mlflow>=1.30.0",
        "dvc>=2.10.0",
        "evidently>=0.1.51",
        "fastapi>=0.85.0",
        "uvicorn>=0.19.0",
        "python-dotenv>=0.19.0",
        "pyyaml>=6.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
            "flake8>=4.0.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "mypy>=0.910",
        ],
        "monitoring": [
            "prometheus-client>=0.14.0",
            "grafana-api>=1.0.4",
        ],
    },
    package_data={
        "": ["*.yaml", "*.yml"],
    },
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "mlops-run=src.api.main:main",
            "mlops-train=src.pipelines.training_pipeline:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)