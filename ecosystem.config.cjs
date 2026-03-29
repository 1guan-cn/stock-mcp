module.exports = {
  apps: [
    {
      name: "stock-service",
      script: "uv",
      args: "run python -m stock_service.main",
      cwd: __dirname,
      interpreter: "none",
      autorestart: true,
      max_restarts: 10,
      restart_delay: 3000,
    },
  ],
};
