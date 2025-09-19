# GitHub Workflows for Owl-Watch

This directory contains GitHub Actions workflows for the Owl-Watch project.

## Workflow Templates

### `setup-homebrew.yml`
A reusable workflow template that installs Homebrew on Ubuntu runners and optionally installs specified packages.

**Parameters:**
- `packages`: Space-separated list of packages to install via Homebrew (optional)
- `update-homebrew`: Whether to update Homebrew before installing packages (default: true)

**Usage:**
```yaml
jobs:
  my-job:
    uses: ./.github/workflows/setup-homebrew.yml
    with:
      packages: 'hatch python@3.11 jq'
      update-homebrew: true
```

## Workflows

### `install-hatch.yml`
Demonstrates using the Homebrew template to install `hatch` (Python build tool) and validates the installation.

**Triggers:**
- Manual dispatch with optional additional packages
- Push to main/master branch affecting pyproject.toml or workflows
- Pull requests affecting pyproject.toml or workflows

### `dev-environment.yml`
Sets up a complete development environment using the Homebrew template, installing both core tools (hatch, Python) and additional development tools.

**Triggers:**
- Manual dispatch with option to include additional dev tools
- Weekly schedule (Mondays at 2 AM UTC)

## Features

- ✅ Installs Homebrew on Ubuntu runners
- ✅ Configures PATH and environment variables correctly
- ✅ Supports installing multiple packages
- ✅ Validates package installations
- ✅ Reusable template design
- ✅ Error handling and verification steps
- ✅ Integration with Owl-Watch project structure

## Example: Installing Additional Tools

To use the template for installing additional tools in your own workflow:

```yaml
jobs:
  setup-tools:
    uses: ./.github/workflows/setup-homebrew.yml
    with:
      packages: 'node@18 aws-cli terraform'
```