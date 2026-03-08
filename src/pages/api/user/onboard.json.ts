import { auth } from "@/lib/auth";
import { db } from "@/db/client";
import { user } from "@/db/schema";
import { eq } from "drizzle-orm";
import type { APIRoute } from "astro";

export const prerender = false;

export const POST: APIRoute = async ({ request }) => {
	const session = await auth.api.getSession({ headers: request.headers });

	if (!session) {
		return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401 });
	}

	try {
		await db.update(user).set({ onboarded: true }).where(eq(user.id, session.user.id));

		return new Response(JSON.stringify({ success: true }), { status: 200 });
	} catch (error) {
		console.error("Failed to update onboarding status:", error);
		return new Response(JSON.stringify({ error: "Internal Server Error" }), { status: 500 });
	}
};
