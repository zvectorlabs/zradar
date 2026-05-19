#!/bin/bash
# Script to enable lld linker after LLVM installation completes
# Run this after: brew install llvm

set -e

echo "🔍 Checking for lld linker..."

# Find lld in homebrew
LLD_PATH=""
if [ -f "$HOME/.homebrew-arm64/opt/llvm/bin/lld" ]; then
    LLD_PATH="$HOME/.homebrew-arm64/opt/llvm/bin/lld"
elif [ -f "/opt/homebrew/opt/llvm/bin/lld" ]; then
    LLD_PATH="/opt/homebrew/opt/llvm/bin/lld"
elif command -v lld &> /dev/null; then
    LLD_PATH=$(which lld)
else
    echo "❌ lld not found. Make sure LLVM installation completed successfully."
    echo "   Run: brew install llvm"
    exit 1
fi

echo "✅ Found lld at: $LLD_PATH"

# Update .cargo/config.toml
CONFIG_FILE=".cargo/config.toml"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ $CONFIG_FILE not found"
    exit 1
fi

echo "📝 Updating $CONFIG_FILE..."

# Comment out current rustflags and uncomment lld
sed -i.bak 's/^rustflags = \["-C", "link-arg=-Wl,-dead_strip"\]/# rustflags = ["-C", "link-arg=-Wl,-dead_strip"]/' "$CONFIG_FILE"
sed -i.bak 's/^# rustflags = \["-C", "link-arg=-fuse-ld=lld"\]/rustflags = ["-C", "link-arg=-fuse-ld=lld"]/' "$CONFIG_FILE"

echo "✅ Configuration updated!"
echo ""
echo "🧪 Testing build with lld..."

# Test build
time cargo build --package zradar-plugin-s3

echo ""
echo "✅ Build optimization complete!"
echo ""
echo "Expected improvements:"
echo "  - Linking step (630/631): ~30s → ~5-10s (60-80% faster)"
echo "  - Full server build: ~2m 23s → ~1m 30s (40% faster)"
echo ""
echo "To verify, run: cargo clean && time cargo build --package zradar-server"
