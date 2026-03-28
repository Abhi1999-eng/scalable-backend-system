import { NextRequest, NextResponse } from "next/server";

const BACKEND_URL = process.env.BACKEND_URL ?? "http://localhost:8082";

async function proxy(request: NextRequest, path: string[]) {
  const targetUrl = new URL(path.join("/"), `${BACKEND_URL.replace(/\/$/, "")}/`);
  request.nextUrl.searchParams.forEach((value, key) => {
    targetUrl.searchParams.append(key, value);
  });

  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("connection");
  headers.delete("content-length");

  const init: RequestInit = {
    method: request.method,
    headers,
    redirect: "manual"
  };

  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = await request.arrayBuffer();
  }

  const response = await fetch(targetUrl, init);
  const responseHeaders = new Headers(response.headers);
  responseHeaders.delete("content-encoding");
  responseHeaders.delete("content-length");
  responseHeaders.delete("transfer-encoding");

  return new NextResponse(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: responseHeaders
  });
}

type Params = Promise<{ path: string[] }>;

export async function GET(request: NextRequest, context: { params: Params }) {
  const { path } = await context.params;
  return proxy(request, path);
}

export async function POST(request: NextRequest, context: { params: Params }) {
  const { path } = await context.params;
  return proxy(request, path);
}

export async function DELETE(request: NextRequest, context: { params: Params }) {
  const { path } = await context.params;
  return proxy(request, path);
}
