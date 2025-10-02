import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  webpack: (config) => {
    // Exclude ostrich_egg symlink from webpack processing
    config.watchOptions = {
      ...config.watchOptions,
      ignored: ['**/ostrich_egg/**', '**/node_modules/**'],
    };
    return config;
  },
};

export default nextConfig;
