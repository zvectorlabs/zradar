# Commit Message Guidelines

We follow the **[Conventional Commits](https://www.conventionalcommits.org/)** specification. This leads to more readable messages that are easy to follow when looking through the project history.

---

## 1. Commit Message Format

Each commit message consists of a **header**, a **body**, and a **footer**. The header has a special format that includes a **type**, a **scope**, and a **subject**:

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

The **header** is mandatory and the scope of the header is optional.

---

## 2. Type

Must be one of the following:

| Type | Description | Semantic Version Impact |
|------|-------------|-------------------------|
| **feat** | A new feature for the user. | `MINOR` |
| **fix** | A bug fix for the user. | `PATCH` |
| **docs** | Documentation-only changes. | `PATCH` (usually) |
| **style** | Code style changes (whitespace, formatting, etc). | `None` |
| **refactor** | A code change that neither fixes a bug nor adds a feature. | `None` |
| **perf** | A code change that improves performance. | `None` (often `PATCH`) |
| **test** | Adding missing tests or correcting existing tests. | `None` |
| **chore** | Routine tasks, maintenance, or build process changes. | `None` |
| **build** | Changes that affect the build system or external dependencies. | `None` |
| **ci** | Changes to CI configuration files and scripts. | `None` |

---

## 3. Scope

The scope should be the name of the crate or component affected.

The following is the list of supported scopes:
- `api`
- `worker`
- `core`
- `models`
- `traits`
- `plugins`
- `postgres`
- `s3`
- `local`
- `deps`
- `wal`
- `config`
- `ci`

There are currently a few exceptions to the "use component name" rule:
- `release`: used when releasing a new version of the package.
- `changelog`: used for updating the changelog.
- `none/empty string`: useful for `style`, `test` and `refactor` changes that are done across all packages (e.g. `style: add missing semicolons`) and for docs changes that are not related to a specific package (e.g. `docs: fix typo in tutorial`).

---

## 4. Subject

The subject contains a succinct description of the change:
- Use the imperative, present tense: "change" not "changed" nor "changes".
- Don't capitalize the first letter.
- No dot (.) at the end.

---

## 5. Body

Just as in the **subject**, use the imperative, present tense: "change" not "changed" nor "changes".
The body should include the motivation for the change and contrast this with previous behavior.

---

## 6. Footer

The footer should contain any information about **Breaking Changes** and is also the place to reference GitHub issues that this commit closes.

**Breaking Changes** should start with the word `BREAKING CHANGE:` with a space or two newlines. The rest of the commit message is then used for this.

---

## 7. Examples

### Good

```
feat(api): add new endpoint for user profile

This adds the GET /users/me endpoint to retrieve the current user's profile.

Closes #123
```

```
fix(postgres): fix connection pool timeout
```

```
chore(deps): update tokio to v1.35
```

### Bad

```
Fixed bug
```
*(Too vague, wrong tense, capitalized)*

```
feat: added new feature.
```
*(Past tense, period at the end)*

---

## 8. Branching Strategy

We use the **[Git Flow](https://nvie.com/posts/a-successful-git-branching-model/)** workflow.

- **`main`**: The production-ready branch. Live code changes are merged here.
- **Feature Branches**: Used for all new development. Create a branch from `main` (e.g., `feat/my-feature`).

### Pull Request Workflow

1.  **Fork** the repository to your own GitHub account.
2.  **Clone** your fork locally.
3.  **Create a branch** for your changes: `git checkout -b feat/my-feature`.
4.  **Commit** your changes following the [Conventional Commits](#1-commit-message-format) guidelines.
5.  **Push** the branch to your fork: `git push origin feat/my-feature`.
6.  **Open a Pull Request** from your fork's branch to the upstream `main` branch.
