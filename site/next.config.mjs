/** @type {import('next').NextConfig} */
const isProd = process.env.NODE_ENV === "production";

const nextConfig = {
  reactStrictMode: true,
  output: "export",
  images: { unoptimized: true },
  trailingSlash: true,
  basePath: isProd ? "/fetchfiles-directory" : "",
  assetPrefix: isProd ? "/fetchfiles-directory/" : "",
};

export default nextConfig;
