import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Ostrich Egg Demo - Privacy Protection Engine",
  description: "Interactive demonstration of Ostrich Egg's small-cell suppression and data privacy protection capabilities",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased">
        {children}
      </body>
    </html>
  );
}
