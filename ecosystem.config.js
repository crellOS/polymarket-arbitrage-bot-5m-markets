module.exports = {
  apps: [{
    name: 'polymarket-bot',
    script: './target/release/polymarket-arbitrage-bot',
    cwd: '/root/rust-pro/polymarket-trading-bot-sports-soccer',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    env: {
      RUST_LOG: 'info'
    },
    error_file: './logs/pm2-error.log',
    out_file: './logs/pm2-out.log'
  }]
};
