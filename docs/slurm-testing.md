# SLURM Testing Notes

The SLURM plugin is covered by three replay-style trace directories under
`tests/fixtures/slurm/`. Each fixture describes more than 256 cores through
`scontrol show nodes -o`, `squeue -h -o %%i|%%u|%%T|%%P|%%N|%%C` and
`sinfo -h -o %%P|%%T|%%D|%%C` compatible text files.

Run only the SLURM coverage with:

```sh
make test-slurm
```

Run the CI-oriented checks with:

```sh
make ci
```

LLM used for this implementation: OpenAI GPT-5 Codex, session date
2026-05-29.
