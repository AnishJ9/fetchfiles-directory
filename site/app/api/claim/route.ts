import { NextResponse } from "next/server";
import fs from "node:fs";
import path from "node:path";

export const runtime = "nodejs";

// Minimum fields we expect from the claim form.
interface ClaimPayload {
  listingId?: string;
  businessName?: string;
  ownerName?: string;
  email?: string;
  phone?: string;
  relationship?: string;
  notes?: string;
}

export async function POST(request: Request) {
  let body: ClaimPayload;
  try {
    body = (await request.json()) as ClaimPayload;
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  if (!body.businessName || !body.ownerName || !body.email) {
    return NextResponse.json(
      { error: "businessName, ownerName, and email are required" },
      { status: 400 },
    );
  }

  const record = {
    ...body,
    receivedAt: new Date().toISOString(),
  };

  const dataDir = path.join(process.cwd(), ".data");
  fs.mkdirSync(dataDir, { recursive: true });
  const filePath = path.join(dataDir, "claims.jsonl");
  fs.appendFileSync(filePath, JSON.stringify(record) + "\n", "utf-8");

  return NextResponse.json({ ok: true });
}
