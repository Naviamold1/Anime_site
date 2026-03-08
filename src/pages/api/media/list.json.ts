import type { APIRoute } from "astro";
import { getList } from "@/db/queries";

export const GET: APIRoute = async ({ url }) => {
  const page = Math.max(1, Number(url.searchParams.get("page") ?? "1") || 1);
  const query = (url.searchParams.get("q") ?? "").trim();
  const data = await getList({ page, query });

  return new Response(JSON.stringify(data), {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
};
