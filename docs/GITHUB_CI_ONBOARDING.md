# GitHub source and CI handover (2026-10-09)

## Confirmed facts

- The local project began as a directory without `.git` metadata.
- The connected GitHub account is `yfj898` and can access 12 repositories.
  None was named `taskflow-go`; an unrelated existing repo is not an
  acceptable destination.
- The connected GitHub API tool supports writes to existing repositories but
  **does not expose repository creation**; on this Linux server `gh auth
  status` initially reported no authenticated GitHub session.
- Consequently, generating local Git commits and running container-backed
  tests cannot honestly be called successful GitHub cloud CI.

## First push to a new private repository

After the owner signs into GitHub CLI **on the remote Linux host**, run:

```bash
gh auth login -h github.com -p https -w
cd /path/to/any/taskflow-go
git status --short
git log -1 --oneline
bash scripts/github-private-bootstrap.sh
```

This refuses a non-main branch, uncommitted changes, a pre-existing remote,
or an existing GitHub repository rather than changing existing projects.
It targets only `yfj898/taskflow-go`, always using **private** visibility.
Do not paste login codes, PATs, tokens or SSH private keys into a chat.

After the new private repo and push are verified, use GitHub's **Settings →
Rules → Rulesets** to require pull requests and successful CI checks before
merging to `main`. Use a branch per subsequent change. Local commits are not
signed attestations and must not be represented as immutable release
provenance.

## CI commands after remote push

```bash
gh run list --repo yfj898/taskflow-go --limit 10
gh run watch --repo yfj898/taskflow-go <run-id>
gh workflow run kind-oidc-fullstack.yml --repo yfj898/taskflow-go --ref main
gh run list --repo yfj898/taskflow-go --workflow kind-oidc-fullstack.yml
```

The standard `ci.yml` includes Go tests, `go vet`, format checks, workflow
schema and tenant-isolation tests, Keycloak actual JWT integration, live HTTP
RBAC checks, a signing-key rotation, IdP outage recovery and a bounded OIDC
SLO gate. The separate `kind-oidc-fullstack.yml` is intentionally manually
triggered because it creates a real disposable Kubernetes cluster.
The production-mode HTTPS reference rehearsal is a separate CI job.

**Do not claim Actions passed until actual GitHub run/job URLs and exit
conclusions have been recorded.** Browser UI state is not a substitute for
the workflow result; GitHub Actions is authoritative.

## Secrets and sensitive files

The repository `.gitignore` excludes binaries, raw profiles, `.env*`,
TLS private keys, PEM certificates, database files and local secrets. Real
passwords are generated at runtime and never included in the Git snapshot.
Before every push, review `git diff --cached`, run a secret scanner (for
example GitHub secret scanning / gitleaks) and confirm actual deployment
secrets are managed outside Git. A basic regex scan is only a preliminary
gate and is not comprehensive.
