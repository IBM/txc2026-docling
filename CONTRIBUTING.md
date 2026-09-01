# Contributing

This repository holds the materials for the Docling sessions at IBM TechXchange
2026. Contributions are welcome — especially from people who ran a lab and hit
something that did not work.

Note that Docling itself is developed elsewhere: bugs and features in the
library belong in the [docling-project/docling](https://github.com/docling-project/docling)
issue tracker, not here. This repo is for the labs, the demos, and their docs.

## What is most useful

* **A step that failed for you.** Open an [issue](https://github.com/IBM/txc2026-docling/issues)
  with the lab number, the command you ran, and the output. If you attended the
  session, say which room/day — some failures are environment-specific.
* **Fixes to the instructions.** Typos, stale URLs, a missing prerequisite, a
  command that only works on one OS. Send a pull request directly; no issue needed.
* **Larger changes** — restructuring a lab, adding a new exercise. Please
  [raise an issue](https://github.com/IBM/txc2026-docling/issues) first so we can
  agree on the shape before you spend time on it. The labs are timed to a
  session slot, so length is a real constraint.

## Scope of a change

Each lab is self-contained in its own directory (`lab-2775/`, …) and has its own
README. Keep a pull request inside one lab where you can; a change that touches
several labs at once is harder to verify during an event.

**Never commit credentials.** The labs read their configuration from a
git-ignored `lab.yaml` (see `lab.yaml.example`). API keys, bucket CRNs, cluster
endpoints and student ids do not belong in the repository — if you are adding a
new setting, add it to the example file with a placeholder value.

## Setup and testing

Each lab documents its own setup in its README. For `lab-2775`, which is a
Python project managed with [uv](https://docs.astral.sh/uv/):

```bash
cd lab-2775
uv sync --group dev
uv run pytest
uv run ruff check .
```

The tests are pure unit tests — they need no cluster, no Kafka and no watsonx
credentials, and they should stay that way, so that anyone can run them before
opening a pull request. If your change touches a shell script or a `setup.sh`
step that the tests cannot cover, say in the pull request how you verified it.

Shell scripts should stay POSIX-ish and readable; Python follows `ruff` with the
line length configured in `pyproject.toml`.

## Merge approval

A pull request needs a review from one maintainer. During the event itself we
may merge fixes quickly and review afterwards — getting a broken step fixed for
the next session matters more than process.

For the list of maintainers, see [MAINTAINERS.md](MAINTAINERS.md).

## Legal

Each source file must include a license header for the Apache Software License
2.0. Using the SPDX format is the simplest approach, e.g.

```
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
```

We use the [Developer's Certificate of Origin 1.1 (DCO)](https://developercertificate.org/)
to manage contributions, the same approach the Linux® Kernel
[community](https://elinux.org/Developer_Certificate_Of_Origin) uses.

We simply ask that when submitting a patch for review, you include a sign-off
statement in the commit message:

```
Signed-off-by: John Doe <john.doe@example.com>
```

Git adds it for you with:

```bash
git commit -s
```

A [DCO bot](https://github.com/probot/dco) checks this on every pull request.

## Communication

Open an [issue](https://github.com/IBM/txc2026-docling/issues) for anything about
these materials, or reach out to a [maintainer](MAINTAINERS.md) directly. If you
are at TechXchange, come find us at the session — that is by far the fastest way.
