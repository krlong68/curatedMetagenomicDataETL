# Contributing to curatedMetagenomicDataETL

Thank you for your interest in contributing to curatedMetagenomicDataETL! This project is part of the [curatedMetagenomicData](https://github.com/waldronlab/curatedMetagenomicData) ecosystem, providing uniformly processed metagenomic datasets to the scientific community.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Submitting Changes](#submitting-changes)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Enhancements](#suggesting-enhancements)
- [Questions](#questions)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates. When creating a bug report, include as many details as possible:

- **Use a clear and descriptive title**
- **Describe the exact steps to reproduce the problem**
- **Provide specific examples** (code snippets, commands, etc.)
- **Describe the behavior you observed** and what you expected
- **Include your environment details** (Python version, OS, etc.)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion:

- **Use a clear and descriptive title**
- **Provide a detailed description** of the suggested enhancement
- **Explain why this enhancement would be useful** to most users
- **List any alternative solutions** you've considered

### Pull Requests

We actively welcome your pull requests:

1. Fork the repo and create your branch from `main`
2. Make your changes following our [coding standards](#coding-standards)
3. Test your changes thoroughly
4. Update documentation as needed
5. Ensure all tests pass
6. Submit a pull request

## Development Setup

### Prerequisites

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- [just](https://github.com/casey/just) task runner
- Google Cloud credentials (for BigQuery operations)

### Initial Setup

```bash
# Clone the repository
git clone https://github.com/seandavi/curatedMetagenomicDataETL.git
cd curatedMetagenomicDataETL

# Install dependencies
uv sync

# Verify installation
just --list
```

### Running the Pipeline

```bash
# See available commands
just --list

# Run individual components
just create-external-tables
just load-reference-tables

# Run the full pipeline
just setup
```

See [CLAUDE.md](./CLAUDE.md) for detailed architecture and [USER_GUIDE.md](./USER_GUIDE.md) for usage examples.

## Coding Standards

### Python Style Guide

- Follow [PEP 8](https://pep8.org/) style guidelines
- Use type hints where appropriate
- Write docstrings for functions and classes
- Keep functions focused and modular

### Code Quality

- **Clarity over cleverness**: Write code that is easy to understand
- **Document complex logic**: Add comments for non-obvious operations
- **Test your changes**: Ensure your code works as expected
- **Handle errors gracefully**: Include appropriate error handling

### Example Code Style

```python
def process_sample_data(sample_id: str, data_type: str) -> dict:
    """
    Process metagenomic sample data for a given type.
    
    Args:
        sample_id: Unique identifier for the sample
        data_type: Type of data to process (e.g., 'taxonomic', 'functional')
    
    Returns:
        Processed data as a dictionary
    
    Raises:
        ValueError: If data_type is not supported
    """
    if data_type not in ['taxonomic', 'functional']:
        raise ValueError(f"Unsupported data type: {data_type}")
    
    # Process data...
    return processed_data
```

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests after the first line

Example:
```
Add support for new marker abundance table

- Implement parser for marker_abundance.tsv.gz files
- Add corresponding BigQuery schema
- Update documentation

Closes #123
```

## Submitting Changes

### Before Submitting

1. **Test your changes**: Ensure the pipeline runs successfully
2. **Update documentation**: Reflect changes in relevant docs
3. **Check code quality**: Follow our coding standards
4. **Review your changes**: Look over the diff before submitting

### Pull Request Process

1. **Create a feature branch**: `git checkout -b feature/your-feature-name`
2. **Make your changes**: Follow coding standards and test thoroughly
3. **Commit your changes**: Use clear, descriptive commit messages
4. **Push to your fork**: `git push origin feature/your-feature-name`
5. **Open a Pull Request**: Provide a clear description of changes

### Pull Request Guidelines

- **Title**: Clear and descriptive
- **Description**: Explain what changes you made and why
- **Link related issues**: Reference any related issues
- **Screenshots**: Include for UI/output changes
- **Testing**: Describe how you tested your changes

## Reporting Bugs

### Bug Report Template

```markdown
**Description**
A clear and concise description of the bug.

**To Reproduce**
Steps to reproduce the behavior:
1. Run command '...'
2. See error

**Expected Behavior**
What you expected to happen.

**Actual Behavior**
What actually happened.

**Environment**
- OS: [e.g., Ubuntu 22.04]
- Python version: [e.g., 3.11.5]
- Package version: [e.g., 0.1.0]

**Additional Context**
Any other context about the problem.
```

## Suggesting Enhancements

### Enhancement Request Template

```markdown
**Is your feature request related to a problem?**
A clear description of the problem. Ex. I'm always frustrated when [...]

**Describe the solution you'd like**
A clear description of what you want to happen.

**Describe alternatives you've considered**
Other solutions or features you've considered.

**Additional context**
Any other context or screenshots about the feature request.
```

## Questions

If you have questions about contributing:

- **Check existing documentation**: [README.md](./README.md), [USER_GUIDE.md](./USER_GUIDE.md), [CLAUDE.md](./CLAUDE.md)
- **Search existing issues**: Someone may have already asked
- **Open a new issue**: Tag it with the `question` label
- **Contact maintainers**: Reach out through GitHub

## Project Structure

```
curatedMetagenomicDataETL/
├── README.md              # Project overview
├── CONTRIBUTING.md        # This file
├── CODE_OF_CONDUCT.md     # Community guidelines
├── USER_GUIDE.md          # Usage documentation
├── CLAUDE.md              # Architecture details
├── justfile               # Task runner commands
├── pyproject.toml         # Python dependencies
├── create_external_tables.py
├── create_src_stg_tables.py
├── load_sample_id_map_to_bigquery.py
├── load_sra_accessions.py
└── gather_table_metadata.py
```

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

## Recognition

Contributors are recognized in the project's commit history and may be acknowledged in release notes for significant contributions.

Thank you for contributing to curatedMetagenomicDataETL! 🎉
