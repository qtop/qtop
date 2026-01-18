# Setup Your Fork for Pull Request

## Step 1: Fork the Correct Repository

You need to fork the **qtop** repository, not OpenVINO:

**Go to:** https://github.com/qtop/qtop

1. Click the "Fork" button in the top right
2. This will create a fork at: `https://github.com/YOUR_USERNAME/qtop`

## Step 2: Add Your Fork as a Remote

Once you've forked, add it as a remote (replace `YOUR_USERNAME` with your GitHub username):

```bash
git remote add fork https://github.com/YOUR_USERNAME/qtop.git
```

## Step 3: Verify Remotes

Check that both remotes are set up:

```bash
git remote -v
```

You should see:
- `origin` pointing to `qtop/qtop.git` (the original)
- `fork` pointing to `YOUR_USERNAME/qtop.git` (your fork)

## Step 4: Push Your Branch to Your Fork

Push the branch with your changes:

```bash
git push -u fork fix/issue-337-pbs-json-parsing
```

## Step 5: Create Pull Request

1. Go to: https://github.com/qtop/qtop
2. You should see a banner suggesting to create a PR
3. Or go directly to: https://github.com/qtop/qtop/compare
4. Select your fork and branch: `tani125:fix/issue-337-pbs-json-parsing`
5. Use the description from `PR_DESCRIPTION.md`

## Quick Commands Summary

```bash
# Add your fork (replace YOUR_USERNAME)
git remote add fork https://github.com/YOUR_USERNAME/qtop.git

# Verify
git remote -v

# Push to your fork
git push -u fork fix/issue-337-pbs-json-parsing
```
