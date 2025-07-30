#!/usr/bin/env python3
"""
Script to generate ML project structure with empty files.
Creates all directories and files without any code content.
"""

import os
from pathlib import Path


def create_file(filepath: str, content: str = ""):
    """Create a file with optional content."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    print(f"Created: {filepath}")


def create_directory(dirpath: str):
    """Create a directory."""
    Path(dirpath).mkdir(parents=True, exist_ok=True)
    print(f"Created directory: {dirpath}")


def generate_ml_project_structure():
    """Generate the complete ML project structure."""
    
    # Root files
    root_files = [
        "README.md",
        "pyproject.toml", 
        ".gitignore",
        ".env.example",
        "Dockerfile",
        "docker-compose.yml"
    ]
    
    for file in root_files:
        create_file(file)
    
    # Configuration files
    config_files = [
        "config/__init__.py",
        "config/config.yaml",
        "config/logging.yaml",
        "config/model_configs/xgboost.yaml",
        "config/model_configs/random_forest.yaml",
        "config/model_configs/neural_net.yaml"
    ]
    
    for file in config_files:
        create_file(file)
    
    # Data directories (empty directories)
    data_dirs = [
        "data/raw",
        "data/interim", 
        "data/processed",
        "data/external"
    ]
    
    for dir_path in data_dirs:
        create_directory(dir_path)
        # Create .gitkeep to preserve empty directories in git
        create_file(f"{dir_path}/.gitkeep")
    
    # Notebook files
    notebook_files = [
        "notebooks/01_exploratory_data_analysis.ipynb",
        "notebooks/02_feature_engineering.ipynb", 
        "notebooks/03_model_experiments.ipynb",
        "notebooks/04_model_evaluation.ipynb"
    ]
    
    for file in notebook_files:
        create_file(file)
    
    # Source code files
    src_files = [
        "src/__init__.py",
        "src/data_pipeline.py",
        "src/feature_pipeline.py", 
        "src/model_pipeline.py",
        "src/utils/__init__.py",
        "src/utils/logging.py",
        "src/utils/config.py",
        "src/utils/metrics.py",
        "src/utils/visualization.py",
        "src/api/__init__.py",
        "src/api/app.py",
        "src/api/schemas.py",
        "src/api/endpoints.py"
    ]
    
    for file in src_files:
        create_file(file)
    
    # Test files
    test_files = [
        "tests/__init__.py",
        "tests/conftest.py",
        "tests/test_data_pipeline.py",
        "tests/test_feature_pipeline.py",
        "tests/test_model_pipeline.py",
        "tests/test_api/__init__.py",
        "tests/test_api/test_endpoints.py"
    ]
    
    for file in test_files:
        create_file(file)
    
    # Model directories (empty directories)
    model_dirs = [
        "models/trained",
        "models/experiments", 
        "models/metadata"
    ]
    
    for dir_path in model_dirs:
        create_directory(dir_path)
        create_file(f"{dir_path}/.gitkeep")
    
    # Reports directories (empty directories)
    report_dirs = [
        "reports/figures",
        "reports/performance",
        "reports/data_quality"
    ]
    
    for dir_path in report_dirs:
        create_directory(dir_path)
        create_file(f"{dir_path}/.gitkeep")
    
    # Script files
    script_files = [
        "scripts/download_data.py",
        "scripts/train_model.py",
        "scripts/evaluate_model.py", 
        "scripts/deploy_model.py",
        "scripts/batch_predict.py"
    ]
    
    for file in script_files:
        create_file(file)
    
    # Deployment files
    deployment_files = [
        "deployment/kubernetes/deployment.yaml",
        "deployment/kubernetes/service.yaml",
        "deployment/kubernetes/ingress.yaml",
        "deployment/terraform/main.tf",
        "deployment/terraform/variables.tf",
        "deployment/terraform/outputs.tf",
        "deployment/monitoring/prometheus.yml",
        "deployment/monitoring/grafana_dashboard.json"
    ]
    
    for file in deployment_files:
        create_file(file)
    
    # Documentation files
    doc_files = [
        "docs/data_dictionary.md",
        "docs/model_documentation.md",
        "docs/api_documentation.md",
        "docs/deployment_guide.md"
    ]
    
    for file in doc_files:
        create_file(file)
    
    print("\n✅ ML project structure generated successfully!")
    print("📁 Total files and directories created")


def generate_gitignore_content():
    """Generate a comprehensive .gitignore file content."""
    return """# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.py,cover
.hypothesis/
.pytest_cache/

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# pyenv
.python-version

# pipenv
Pipfile.lock

# PEP 582
__pypackages__/

# Celery stuff
celerybeat-schedule
celerybeat.pid

# SageMath parsed files
*.sage.py

# Environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# Spyder project settings
.spyderproject
.spyproject

# Rope project settings
.ropeproject

# mkdocs documentation
/site

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# Pyre type checker
.pyre/

# ML specific
models/trained/*
!models/trained/.gitkeep
data/raw/*
!data/raw/.gitkeep
data/interim/*
!data/interim/.gitkeep
data/processed/*
!data/processed/.gitkeep
data/external/*
!data/external/.gitkeep
reports/figures/*
!reports/figures/.gitkeep
reports/performance/*
!reports/performance/.gitkeep
reports/data_quality/*
!reports/data_quality/.gitkeep

# MLflow
mlruns/
mlartifacts/

# Weights & Biases
wandb/

# TensorBoard
logs/
tensorboard_logs/

# Model files
*.pkl
*.joblib
*.h5
*.pb
*.onnx
*.tflite

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Docker
.dockerignore
"""


def generate_env_example_content():
    """Generate .env.example file content."""
    return """# Environment Configuration Template
# Copy this to .env and fill in your values

# Database
DATABASE_URL=postgresql://user:password@localhost:5432/mlproject
DATABASE_ECHO=false

# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=your-ml-bucket

# MLflow Configuration  
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT_NAME=ml-project-experiment

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_DEBUG=false

# Model Configuration
MODEL_REGISTRY_URI=s3://your-model-bucket/models
DEFAULT_MODEL_NAME=production_model

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Redis (for caching)  
REDIS_URL=redis://localhost:6379/0

# Monitoring
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000

# Security
SECRET_KEY=your-secret-key-here
JWT_SECRET=your-jwt-secret-here
"""


def generate_readme_content():
    """Generate README.md file content."""
    return """# ML Project

A comprehensive machine learning pipeline project with MLOps best practices.

## Project Structure

```
ml_project/
├── src/                    # Source code
├── tests/                  # Test files  
├── notebooks/              # Jupyter notebooks
├── config/                 # Configuration files
├── data/                   # Data storage
├── models/                 # Model artifacts
├── scripts/                # Utility scripts
├── deployment/             # Deployment configs
└── docs/                   # Documentation
```

## Setup

1. Clone the repository
2. Create virtual environment: `python -m venv venv`
3. Activate environment: `source venv/bin/activate` (Linux/Mac) or `venv\\Scripts\\activate` (Windows)
4. Install dependencies: `pip install -e ".[dev]"`
5. Copy `.env.example` to `.env` and configure

## Usage

### Development
```bash
# Run tests
pytest

# Format code
black src/
isort src/

# Type checking
mypy src/

# Start API server
ml-serve
```

### Training
```bash
# Train model
ml-train

# Evaluate model  
ml-evaluate

# Batch predictions
ml-predict
```

## Contributing

1. Install pre-commit hooks: `pre-commit install`
2. Create feature branch
3. Make changes with tests
4. Submit pull request

## License

MIT License
"""


if __name__ == "__main__":
    # Change to the directory where you want to create the project
    # os.chdir("/path/to/your/projects")  # Uncomment and modify as needed
    
    print("🚀 Generating ML project structure...")
    generate_ml_project_structure()
    
    # Generate content for key files
    create_file(".gitignore", generate_gitignore_content())
    create_file(".env.example", generate_env_example_content())  
    create_file("README.md", generate_readme_content())
    
    print("\n📋 Next steps:")
    print("1. Copy .env.example to .env and configure")
    print("2. Update pyproject.toml with your details")
    print("3. Install dependencies: pip install -e '.[dev]'")
    print("4. Initialize git: git init")
    print("5. Install pre-commit: pre-commit install")
    print("6. Start coding! 🎉")