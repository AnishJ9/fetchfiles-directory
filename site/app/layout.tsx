import type { Metadata } from "next";
import "./globals.css";
import { Header } from "@/components/Header";
import { Footer } from "@/components/Footer";

export const metadata: Metadata = {
  title: {
    default: "Fetch Directory — Pet services in your city",
    template: "%s | Fetch Directory",
  },
  description:
    "Find vets, groomers, boarders, daycares, and sitters in Atlanta, Tampa, Austin, Nashville, and Asheville.",
  openGraph: {
    title: "Fetch Directory",
    description:
      "Find vets, groomers, boarders, daycares, and sitters across five metros.",
    type: "website",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen flex flex-col">
        <Header />
        <main className="flex-1">{children}</main>
        <Footer />
      </body>
    </html>
  );
}
