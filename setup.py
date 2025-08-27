#!/usr/bin/env python3
"""
Setup script for ML Project.
"""

import os
from pathlib import Path
from setuptools import setup, find_packages # type: ignore

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

# Read requirements from requirements.txt
def read_requirements(filename):
    """Read requirements from file and return as list."""
    requirements_path = this_directory / filename
    if requirements_path.exists():
        with open(requirements_path, 'r', encoding='utf-8') as f:
            requirements = []
            for line in f:
                line = line.strip()
                # Skip empty lines, comments, and -r flags
                if line and not line.startswith('#') and not line.startswith('-r'):
                    # Remove inline comments
                    if '#' in line:
                        line = line.split('#')[0].strip()
                    if line:  # Check if line still has content after removing comments
                        requirements.append(line)
            return requirements
    return []

# Core requirements (required for basic functionality)
install_requires = [
    # Data manipulation and analysis
    "pandas>=1.5.0",
    "numpy>=1.21.0", 
    "scikit-learn>=1.2.0",
    
    # Configuration management
    "pyyaml>=6.0",
    "python-dotenv>=0.19.0",
    
    # Logging
    "loguru>=0.6.0",
    
    # Visualization
    "matplotlib>=3.5.0",
    "seaborn>=0.11.0",
    
    # API framework
    "fastapi>=0.85.0",
    "uvicorn>=0.18.0",
    
    # Data validation
    "pydantic>=1.10.0",
    
    # Additional utilities
    "requests>=2.28.0",
    "tqdm>=4.64.0",
]

# Optional dependencies
extras_require = {
    # Machine learning frameworks
    'ml': [
        "xgboost>=1.6.0",
        "lightgbm>=3.3.0",
        "pandera>=0.15.0",
        "hydra-core>=1.2.0",
    ],
    
    # Deep learning frameworks (heavy dependencies)
    'dl': [
        "torch>=1.12.0", 
        "tensorflow>=2.10.0",
    ],
    
    # Cloud and database
    'cloud': [
        "boto3>=1.26.0",
        "sqlalchemy>=1.4.0",
        "psycopg2-binary>=2.9.0",
    ],
    
    # Experiment tracking
    'tracking': [
        "mlflow>=2.0.0",
        "wandb>=0.13.0",
    ],
    
    # Additional visualization
    'viz': [
        "plotly>=5.10.0",
    ],
    
    # Development dependencies
    'dev': [
        "pytest>=7.0.0",
        "pytest-cov>=4.0.0", 
        "pytest-mock>=3.8.0",
        "black>=22.0.0",
        "isort>=5.10.0",
        "flake8>=5.0.0",
        "mypy>=0.991",
        "pre-commit>=2.20.0",
        "bandit>=1.7.0",
        "safety>=2.2.0",
    ],
    
    # Documentation
    'docs': [
        "mkdocs>=1.4.0",
        "mkdocs-material>=8.5.0", 
        "mkdocstrings[python]>=0.19.0",
    ],
    
    # Jupyter notebooks
    'notebooks': [
        "jupyter>=1.0.0",
        "ipykernel>=6.15.0",
        "ipywidgets>=8.0.0",
        "jupyterlab>=3.4.0",
        "nbstripout>=0.6.0",
    ],
    
    # Performance profiling
    'profiling': [
        "memory-profiler>=0.60.0",
        "line-profiler>=4.0.0",
    ],
}

# Convenience extras
extras_require['all'] = list(set([
    dep for extra in extras_require.values() 
    for dep in extra
]))

setup(
    name="ml-project",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A comprehensive machine learning pipeline project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/ml-project",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/ml-project/issues",
        "Documentation": "https://yourusername.github.io/ml-project",
        "Source Code": "https://github.com/yourusername/ml-project",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10", 
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.9",
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        "console_scripts": [
            "ml-train=src.scripts.train_model:main",
            "ml-evaluate=src.scripts.evaluate_model:main", 
            "ml-predict=src.scripts.batch_predict:main",
            "ml-serve=src.api.app:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.json", "*.txt"],
    },
    zip_safe=False,
    keywords="machine-learning mlops data-science pipeline",
)