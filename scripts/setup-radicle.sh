#!/bin/sh
# Don't use set -e to avoid terminal closing when sourced with . ./scripts/setup-radicle.sh
# Instead, check return codes explicitly and report errors

# Check if rad command is available
if ! command -v rad >/dev/null 2>&1; then

  echo "⛓️‍💥 This script requires the Radicle toolchain to be installed."
  echo "  💡 Please install it from https://radicle.xyz/download and try again."
  exit 1
fi

echo "⚙️ Setting up repository..."

# Get repository root directory using git
REPO_ROOT="$(git rev-parse --show-toplevel)"

echo "🎛️ Configuring 'git patch' command..."
# adds git alias to create a patch
git config --local alias.patch '!git push rad HEAD:refs/patches -o patch.message="$1" && git push -u upstream $(git rev-parse --abbrev-ref HEAD); #'

# extract GitHub username and repo from YAML file
GITHUB_ACTIONS_FILE="$REPO_ROOT/.radicle/github_actions.yaml"
if [ ! -f "$GITHUB_ACTIONS_FILE" ]; then
  echo "⛓️‍💥 Could not find GitHub actions config file at: $GITHUB_ACTIONS_FILE"
  exit 1
fi

GITHUB_USERNAME=$(grep "github_username:" "$GITHUB_ACTIONS_FILE" | cut -d ":" -f2 | tr -d ' ')
if [ -z "$GITHUB_USERNAME" ]; then
  echo "⛓️‍💥 Could not extract github_username from $GITHUB_ACTIONS_FILE"
  echo "    This variable is required to configure GitHub remotes."
  exit 1
fi

GITHUB_REPO=$(grep "github_repo:" "$GITHUB_ACTIONS_FILE" | cut -d ":" -f2 | tr -d ' ')
if [ -z "$GITHUB_REPO" ]; then
  echo "⛓️‍💥 Could not extract github_repo from $GITHUB_ACTIONS_FILE"
  echo "    This variable is required to configure GitHub remotes."
  exit 1
fi

# Extract RID from radicle.yaml
RADICLE_FILE="$REPO_ROOT/.radicle/radicle.yaml"
if [ ! -f "$RADICLE_FILE" ]; then
  echo "⛓️‍💥 Could not find Radicle config file at: $RADICLE_FILE"
  exit 1
fi

RID=$(grep "RADICLE_REPOSITORY_ID:" "$RADICLE_FILE" | cut -d ":" -f2 | tr -d ' ')
if [ -z "$RID" ]; then
  echo "⛓️‍💥 Could not extract RADICLE_REPOSITORY_ID from $RADICLE_FILE"
  echo "    This variable is required to configure Radicle remotes."
  exit 1
fi

NODE_ID=$(rad self --nid)
if [ -z "$NODE_ID" ]; then
  echo "⛓️‍💥 Could not get Radicle Node ID using 'rad self --nid'"
  echo "    This is required to configure Radicle remotes."
  exit 1
fi

# Define reusable URL variables
RAD_FETCH_URL="rad://$RID"
RAD_PUSH_URL="rad://${RID}/${NODE_ID}"
GITHUB_PUSH_URL="https://github.com/${GITHUB_USERNAME}/${GITHUB_REPO}.git"



echo "⛓️ Setting up remotes to automate CI flows for submitted patches..."

# Check if upstream remote is already configured
if git remote | grep -q "^upstream$"; then
  # Check upstream remote configuration
  UPSTREAM_URL=$(git remote get-url upstream 2>/dev/null)

  if [ "$UPSTREAM_URL" != "$RAD_FETCH_URL" ]; then
    echo "  ⚠️  'upstream' remote URL doesn't match expected value."
    echo "      Current: $UPSTREAM_URL"
    echo "      Expected: $RAD_FETCH_URL"
    echo "  🚫 Aborting to avoid overwriting your customized remote configuration."
    exit 1
  fi

  # Check push URLs

  # Get all push URLs for upstream (including all configured push URLs)
  PUSH_URLS=$(git remote get-url --all --push upstream 2>/dev/null | tr '\n' ' ')

  # Check if GitHub URL is configured
  # Use a looser grep check to match the GitHub URL regardless of exact format
  GITHUB_URL_FOUND=0
  if echo "$PUSH_URLS" | grep -q "github.com/${GITHUB_USERNAME}/${GITHUB_REPO}"; then
    GITHUB_URL_FOUND=1
  fi

  if [ "$GITHUB_URL_FOUND" -eq 0 ]; then
    echo "  ⚠️  GitHub push URL is not configured for upstream."
    echo "      Expected: $GITHUB_PUSH_URL"
    echo "  🚫 Aborting to avoid overwriting your customized remote configuration."
    exit 1
  fi

  # Check if any Radicle URL is configured
  RAD_URL_FOUND=0
  if echo "$PUSH_URLS" | grep -q "rad://"; then
    RAD_URL_FOUND=1
  fi

  if [ "$RAD_URL_FOUND" -eq 0 ]; then
    echo "  ⚠️  No Radicle push URL is configured for upstream."
    echo "      Expected: $RAD_PUSH_URL"
    echo "  🚫 Aborting to avoid overwriting your customized remote configuration."
    exit 1
  fi

  # upstream remote is properly configured
else
  echo "  ⛓️ Adding 'upstream' remote"
  git remote add upstream "$RAD_FETCH_URL"
  git remote set-url --push upstream "$GITHUB_PUSH_URL"
  git remote set-url --add --push upstream "$RAD_PUSH_URL"
fi

# Check/configure 'rad' remote
if git remote | grep -q "^rad$"; then
  # Check rad remote configuration
  RAD_URL=$(git remote get-url rad 2>/dev/null)

  if [ "$RAD_URL" != "$RAD_FETCH_URL" ]; then
    echo "  ⚠️  'rad' remote URL doesn't match expected value."
    echo "      Current: $RAD_URL"
    echo "      Expected: $RAD_FETCH_URL"
    echo "  🚫 Aborting to avoid overwriting your customized remote configuration."
    exit 1
  fi

  # Check if any Radicle push URL exists (less strict check)
  # Get all push URLs for rad and combine them
  RAD_PUSH_URLS=$(git remote get-url --all --push rad 2>/dev/null | tr '\n' ' ' || echo "")

  # Check if any line contains a Radicle URL
  RAD_URL_FOUND=0
  if echo "$RAD_PUSH_URLS" | grep -q "rad://"; then
    RAD_URL_FOUND=1
  fi

  if [ "$RAD_URL_FOUND" -eq 0 ]; then
    echo "  ⚠️  'rad' push URL is not a Radicle URL."
    echo "      Expected: A URL starting with 'rad://'"
    echo "  🚫 Aborting to avoid overwriting your customized remote configuration."
    exit 1
  fi

  # rad remote is properly configured
else
  echo "  ⛓️ Adding 'rad' remote"
  git remote add rad "$RAD_FETCH_URL"
  git remote set-url --push rad "$RAD_PUSH_URL"
fi

echo "👾 Success! You can now use 'git patch \"Your PR title\"' to submit pull requests"
