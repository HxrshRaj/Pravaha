/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  reactStrictMode: true,
  poweredByHeader: false,
  async rewrites() {
    const internal = process.env.PRAVAHA_API_INTERNAL_URL;
    // In Docker, proxy /proxy-api/* to the api service so the browser can talk
    // to the backend without CORS when NEXT_PUBLIC_API_BASE_URL is same-origin.
    return internal
      ? [{ source: "/proxy-api/:path*", destination: `${internal}/:path*` }]
      : [];
  },
};
module.exports = nextConfig;
