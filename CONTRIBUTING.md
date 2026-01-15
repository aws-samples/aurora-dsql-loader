# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.


## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass.
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).


## Development Setup

This project is written in Rust. To contribute, you'll need:

### Prerequisites

1. **Rust 1.85+**: Install from https://rustup.rs/
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

   The project uses edition 2024, so Rust 1.85 or later is required.

2. **AWS CLI**: For testing (optional)
   ```bash
   # Install AWS CLI v2
   # See: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
   ```

3. **Aurora DSQL cluster**: For integration testing (optional)

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release
```

The compiled binary will be at `target/release/aurora-dsql-loader`.

### Running Tests

```bash
# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test
cargo test test_name
```

### Code Quality

Before submitting a pull request, ensure your code passes all checks:

```bash
# Format code
cargo fmt

# Check formatting without modifying files
cargo fmt -- --check

# Run clippy (linter)
cargo clippy --all-targets --all-features -- -D warnings

# Run all checks
cargo fmt -- --check && \
cargo clippy --all-targets --all-features -- -D warnings && \
cargo test
```

### Running Locally

```bash
# Run with cargo
cargo run -- load --endpoint xxx --source-uri data.csv --table my_table

# Or use the built binary
./target/release/aurora-dsql-loader load --endpoint xxx --source-uri data.csv --table my_table
```

### Project Structure

```
aurora-dsql-loader/
├── src/
│   ├── main.rs           # Entry point
│   ├── config.rs         # Configuration and CLI args
│   ├── runner.rs         # Main load runner logic
│   ├── coordination/     # Load coordination and workers
│   ├── db/               # Database connection and schema
│   ├── formats/          # File format parsers (CSV, TSV, Parquet)
│   └── io/               # I/O abstractions (local, S3)
├── Cargo.toml            # Dependencies and metadata
└── README.md             # User documentation
```

### Debugging

Enable debug logging:
```bash
RUST_LOG=debug cargo run -- load --endpoint xxx --source-uri data.csv --table my_table
```

Log levels: `error`, `warn`, `info`, `debug`, `trace`

### CI/CD

All pull requests run through GitHub Actions CI which:
- Checks code formatting with `cargo fmt`
- Runs linting with `cargo clippy`
- Builds the project
- Runs the test suite
- Builds on Linux and macOS

Ensure your code passes CI before requesting review.

## Publishing a Release

Releases are automated through GitHub Actions. When you push a version tag, the workflow will:
1. Run all tests to ensure code quality
2. Build binaries for multiple platforms
3. Create a GitHub release with all artifacts

### Creating a Release

1. **Update version in Cargo.toml:**
   ```bash
   # Edit version field
   vim Cargo.toml
   ```

2. **Commit version bump:**
   ```bash
   git add Cargo.toml
   git commit -m "Bump version to X.Y.Z"
   git push origin main
   ```

3. **Create and push a version tag:**
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

4. **Monitor the release workflow:**
   - Go to: https://github.com/aws-samples/aurora-dsql-loader/actions
   - The release workflow will run automatically
   - Check for any failures


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' issues is a great place to start.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.
