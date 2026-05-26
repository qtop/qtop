# Challenge #337 Differential Debugging Report

## Goal
Compare one working and one failing qtop execution environment.

## qtop version tested
See version.txt

## Working environment
- Ubuntu via WSL
- Python 3.14
- qtop demo mode executed successfully

## Failing environment
- Windows CMD
- Python 3.14

## Initial differences observed

### Issue 1: Bash launcher incompatibility
The repository launcher script (`qtop`) uses Bash syntax and fails under Windows CMD.

Error:
SyntaxError near:
python3 -m qtop_py.cli "$@"

### Issue 2: SIGPIPE import incompatibility
Running qtop directly with Windows Python failed because SIGPIPE is unavailable on Windows.

Error:
ImportError: cannot import name 'SIGPIPE'

### Issue 3: CRLF line ending issue
The launcher script contained Windows CRLF line endings, causing:

/bin/bash^M: bad interpreter

This was fixed using:
sed -i 's/\r$//' qtop

## Working run
See:
- working-output.txt

## Possible cause
qtop currently assumes a Unix/Linux execution environment.

## Is bug present in latest version?
Yes

## Notes
qtop runs successfully inside Ubuntu/WSL.
