# TaskFlow supply-chain release policy

Production releases must satisfy all of the following before promotion:

1. The container is built from the repository Dockerfile with OCI source, version and revision labels.
2. The release image is referenced by immutable digest, never only by a mutable tag.
3. An SPDX JSON SBOM is generated for the exact image digest.
4. Trivy reports no fixed `CRITICAL` image vulnerability. Exceptions require a documented, time-bounded risk acceptance outside this repository.
5. GitHub Actions publishes build provenance and SBOM attestations for the digest.
6. The digest is signed keylessly with Cosign using GitHub OIDC; verification must constrain both certificate identity and issuer to the repository workflow.
7. Kubernetes production manifests pin the verified digest. A different digest requires a new verification cycle.

The release workflow deliberately separates pull-request scanning from signing. Pull requests may build and scan, but only trusted pushes to `main` can obtain the GitHub OIDC identity used for release signing and attestations.
