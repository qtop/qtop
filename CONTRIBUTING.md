## AI-assisted contributions

AI tools are welcome when used constructively to improve software quality,
documentation, testing, portability, maintainability or user experience.

However:

- Contributors remain fully responsible for all submitted code.
- Contributors must understand and be able to explain the submitted changes.
- Contributors must validate correctness with appropriate testing.
- Mass-generated or low-understanding submissions may be rejected.
- Autonomous bot-driven pull requests are not accepted.

If AI assistance materially contributed to a change, disclose it briefly in the pull request description.

## General Conventions

Please follow common conventions for Open Source projects, f.i. align to Electron framework if still in doubt:
- AI version disclosure is mandatory; with version, not just a name
- `develop` branch should be used as source and target of PRs
- avoid dependencies at all times: qtop is oftentimes run in protoclusters, before any more packages have arrived
- provide at least one screenshot proving good conformance on python3.6/rhel8 because several clusters still use that
- For source code contributions either a Developer Certificate of Origin (DCO) [1] [2] or a Contributor License Agreement (CLA) [3] may be acceptable. DCO is now enforced across the qtop project, so please align to it
- A good bug report should be actionable, concise and detailed, allowing developers to reproduce the issue immediately
- For new features or fixes, either open a new issue or leave a comment on a relevant case that is already open
- Let's avoid storing artifacts in the main `qtop` repo and keep it light; use the repo `qtop-artifacts` instead.

## Proof of humanity

Due to many incoming PRs, it is needed to extend the effort to deal with the advent of bots; part of this is yours.
You give a greater chance of closer and faster attention to your PRs by showing some real humane effort:
- Add (ORCID) in your PR subject iff your github profile is linked to your ORCID profile
- Add (SCHOLAR) in your PR subject iff you have a verifiable google scholar profile
- Add (LI) in your PR subject iff you have a VERIFIABLE Linkedin profile which is shared under github
- Add (SDP) iff you have a Student Developer Pack - if you are eligible and don't have this, now is the time!
- Add (PRO) iff you have a github pro account
- Add (human) iff you'd accept any challenge for PoH, including a live video call / >1 proof channels etc. No sending of personal data/documents though, that's a no-go zone. No bits of hard feelings :)

You may contribute in the following ways:
* Write code
* Review pull requests
* Maintain and improve a qtop repo and/or documentation
* Help with outreach and onboard new contributors by assisting them directly
* Write and/or lead collaborations proposals, including grants or other fundraising or help with community efforts

[1] https://wiki.linuxfoundation.org/dco

[2] https://developercertificate.org/

[3] https://en.wikipedia.org/wiki/Contributor_License_Agreement

[4] https://www.conventionalcommits.org/en/v1.0.0/ or https://www.electronjs.org/docs/latest/development/pull-requests#step-5-commit
