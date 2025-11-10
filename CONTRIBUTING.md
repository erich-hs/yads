# Contributing to yads

We welcome contributions to yads! Whether you're fixing a bug, adding a feature, or improving documentation, your help is appreciated. This guide will walk you through the contribution process.

If anything is unclear after reading this guide, feel free to open an [issue](https://github.com/erich-hs/yads/issues) and ask for clarification.

## Found a bug?

Bug reports help us improve yads. Before submitting a bug report:

- Search [existing issues](https://github.com/erich-hs/yads/issues) to avoid duplicates
- Verify the bug exists on the latest version of yads
- If you find a similar closed issue, open a new one and reference it

When reporting bugs, include enough detail to help us reproduce the issue. The more information you provide, the faster we can investigate and fix the problem.

## Have a feature idea?

We track feature requests through [GitHub issues](https://github.com/erich-hs/yads/issues). Before suggesting a feature:

- Check if someone has already requested something similar
- Think about how the feature aligns with yads' goals—You can read more about it in the [README](README.md). If unclear, suggest the feature anyways! We love hearing new ideas.

In your feature request, explain what you want to achieve and why it would be valuable. Show examples of how you envision using the feature.

## Contributing code

### Finding something to work on

Browse the [issue tracker](https://github.com/erich-hs/yads/issues) to find tasks that interest you. Look for issues that aren't assigned to anyone. The yads codebase covers schema specification, type systems, and converters for multiple frameworks, so there's plenty of variety.

Start with smaller issues to get familiar with the codebase architecture. Once you've picked an issue, comment on it to let others know you're working on it. Use the issue thread to discuss your approach if needed.

### Environment setup

Contributing to yads requires [Python](https://www.python.org/) and [uv](https://github.com/astral-sh/uv) for dependency management.

#### Get the code

You'll need a [GitHub account](https://github.com) and [git](https://git-scm.com) installed. Fork the yads repository on GitHub, then clone your fork:

```bash
git clone https://github.com/<your-username>/yads.git
cd yads
```

Add the main repository as a remote to keep your fork synchronized:

```bash
git remote add upstream https://github.com/erich-hs/yads.git
git fetch upstream
```

#### Install dependencies

Install Python 3.10 or newer. We recommend using [uv to manage Python versions](https://docs.astral.sh/uv/concepts/python-versions/).

Install uv if you haven't already:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Run the test suite to verify your setup:

```bash
make test
```

This command creates a virtual environment, installs dependencies, and runs tests. The first run will be slow as it sets everything up.

Verify linting works:

```bash
make lint
```

We use [ruff](https://github.com/charliermarsh/ruff) for code formatting and linting. The `make lint` command runs these checks automatically.

If everything passes, your development environment is ready.

#### Keeping dependencies current

Update your environment regularly to avoid conflicts with CI. Sync your fork with the main repository:

```bash
git checkout main
git fetch upstream
git rebase upstream/main
git push origin main
```

Update dependencies:

```bash
uv sync --all-groups
```

### Making changes

Create a new branch from `main` for your work:

```bash
git checkout -b feature/your-feature main
```

The source code lives in `src/yads/`. Use the Makefile commands while developing:

- `make test` - Run the test suite
- `make lint` - Check formatting and run linters  
- `make format` - Auto-format code

Your changes won't be merged if tests fail or linting issues exist. Run `make help` to see all available commands.

Remember to:

- Add tests for new functionality
- Update docstrings if you change the public API

### Testing your changes

Tests are organized by module in the `tests/` directory. Write tests that are clear and comprehensive. Use descriptive names that explain what behavior is being verified.

yads tests compatibility with multiple versions of optional dependencies (PySpark, PyArrow, Pydantic, Polars). See the [CI README](ci/README.md) for details on dependency testing and integration tests.

### Submitting your work

Open a [pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) when your changes are ready. Follow these guidelines:

**Pull request title:** Use [conventional commit](https://www.conventionalcommits.org/) format with the [Angular convention](https://github.com/angular/angular/blob/22b96b9/CONTRIBUTING.md#type). Start with a type prefix like `feat:`, `fix:`, `docs:`, etc. Write a clear description that makes sense to users reading the changelog. Capitalize the first letter and avoid ending with punctuation. Annotate code references with backticks.

Example: ``fix: Handle null values correctly in nested struct fields``

**Pull request description:** Link to the issue you're addressing. Provide context that helps reviewers understand your changes. Explain any design decisions or trade-offs.

**Before submitting:**

- Rebase your branch on the latest `main`
- Ensure all CI checks pass
- Verify tests and linting pass locally

A maintainer will review your pull request and may suggest changes. Once approved, we'll merge it using "Squash and merge". This keeps the git history clean with one commit per feature.

Don't worry about making everything perfect on the first try. Open a draft pull request if you want feedback on your approach.

## Code conventions

yads follows standard Python practices:

- [PEP 8](https://pep8.org/) style guide
- Type hints following [PEP 484](https://www.python.org/dev/peps/pep-0484/)
- Maximum line length of 90 characters
- Modern Python idioms (requires Python 3.10+)
- Google-style docstrings for user-facing code
- Self-documenting names for internal code

Use named imports for the `spec` and `types` modules to prevent name collisions with common terms like Field, String, etc.

Run `make format` before committing to automatically fix formatting issues.

## Development model

yads uses trunk-based development. The `main` branch is always stable and ready to deploy. All development happens in short-lived feature branches that get merged via pull requests.

Branch naming suggestions:
- `feature/descriptive-name` for new functionality
- `fix/descriptive-name` for bug fixes

Keep branches focused on a single change. Merge early and often to minimize conflicts. Delete branches after they're merged.

We use "Squash and merge" for all pull requests. This creates a single commit on `main` per feature, keeping history easy to navigate. The PR title becomes the commit message, which is why conventional commit format matters for release notes.

## Releasing new versions

Maintainers handle releases. The process uses [Release Drafter](https://github.com/release-drafter/release-drafter) to automatically generate release notes from pull request titles. For complete release procedures, see [RELEASE.md](RELEASE.md).

Contributors don't need to worry about versions or releases - just focus on your code changes.

## License

Your contributions will be licensed under the same terms as the yads project.
