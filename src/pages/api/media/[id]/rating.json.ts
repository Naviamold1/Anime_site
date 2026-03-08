import type { APIRoute } from "astro";
import { db } from "@/db/client";
import { ratings } from "@/db/schema";
import { auth } from "@/lib/auth";
import { and, avg, count, eq } from "drizzle-orm";

export const prerender = false;

export const GET: APIRoute = async ({ params, request }) => {
	const mediaId = Number(params.id);

	if (!mediaId) return new Response("Invalid ID", { status: 400 });

	const session = await auth.api.getSession({ headers: request.headers });

	// Get average rating and count
	const stats = await db
		.select({
			average: avg(ratings.score),
			total: count(ratings.id),
		})
		.from(ratings)
		.where(eq(ratings.mediaId, mediaId));

	// If user is logged in, check their rating
	let userRating = null;
	if (session?.user?.id) {
		const userRatingRow = await db
			.select()
			.from(ratings)
			.where(and(eq(ratings.mediaId, mediaId), eq(ratings.userId, session.user.id)))
			.limit(1);

		if (userRatingRow.length > 0) {
			userRating = userRatingRow[0].score;
		}
	}

	return new Response(
		JSON.stringify({
			stats: {
				average: stats[0]?.average ? Number(stats[0].average).toFixed(1) : null,
				total: stats[0]?.total || 0,
			},
			userRating,
		}),
		{
			status: 200,
			headers: { "Content-Type": "application/json" },
		},
	);
};

export const POST: APIRoute = async ({ params, request }) => {
	const session = await auth.api.getSession({ headers: request.headers });
	if (!session?.user) {
		return new Response("Unauthorized", { status: 401 });
	}

	const mediaId = Number(params.id);
	if (!mediaId) return new Response("Invalid ID", { status: 400 });

	const body = await request.json();
	const score = Number(body.score);

	if (isNaN(score) || score < 1 || score > 10) {
		return new Response("Invalid score", { status: 400 });
	}

	// Upsert rating using better-sqlite3 insert/onConflict logic via drizzle, or just check and update
	// Since we don't have a unique constraint on (userId, mediaId) in schema (which we should have had!),
	// let's do a manual check.
	const existing = await db
		.select()
		.from(ratings)
		.where(and(eq(ratings.mediaId, mediaId), eq(ratings.userId, session.user.id)))
		.limit(1);

	if (existing.length > 0) {
		await db
			.update(ratings)
			.set({ score, createdAt: new Date() })
			.where(eq(ratings.id, existing[0].id));
	} else {
		// We need unique IDs for table 'ratings'. We can use crypto.randomUUID()
		await db.insert(ratings).values({
			id: crypto.randomUUID(),
			userId: session.user.id,
			mediaId,
			score,
			createdAt: new Date(),
		});
	}

	return new Response(JSON.stringify({ success: true }), { status: 200 });
};
