<!-- Thank you for your contribution! -->
## Changes

Fixes #. <!-- Provide issue number if exists -->

<!-- Please give a short brief about these changes. -->

## Checklist

If this is a user-facing code change, like a bugfix or a new feature, please ensure that
you've fulfilled the following conditions (where applicable):

- [ ] You've added tests (in `tests/`) added which would fail without your patch
- [ ] You've updated the documentation (in `docs/`, in case of behavior changes or new
features)
- [ ] You've added a new changelog entry (in `docs/versionhistory.rst`).

If this is a trivial change, like a typo fix or a code reformatting, then you can ignore
these instructions.

### Updating the changelog

If there are no entries after the last release, use `**UNRELEASED**` as the version.
If, say, your patch fixes issue #999, the entry should look like this:

`* Fix big bad boo-boo in the async scheduler (#999
<https://github.com/agronholm/apscheduler/issues/999>_; PR by @yourgithubaccount)`

If there's no issue linked, just link to your pull request instead by updating the
changelog after you've created the PR.
