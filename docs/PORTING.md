# Moving improvements between private and public repositories

The repositories must remain unrelated Git histories. Move changes as reviewed
patches, never by merging histories or adding the private repository as a public
remote.

For a private-to-public improvement:

1. Identify the smallest production change and its regression test in private.
2. Reimplement or export a patch containing only those files; omit results,
   prompts, machine paths, issue links and research metadata.
3. Apply it on a public feature branch, adapt package/module paths, and review
   the complete diff for sensitive context.
4. Run formatting, build, vet, race tests, integration tests, dependency and
   secret scans in the public tree.
5. Record provenance as a neutral technical note, not a private commit hash.

For a public-to-private fix, apply a reviewed patch to the private research
branch and rerun the private benchmark gates. This keeps validation evidence
private while allowing the implementation and regression test to converge.

Do not use `git replace`, grafts, subtree history imports, commit-range pushes,
or mirrored tags. A patch queue or manually curated changeset makes the privacy
boundary visible and auditable.
