import type { APIRoute } from "astro";
import { db } from "@/db/client";
import { comments, user } from "@/db/schema";
import { auth } from "@/lib/auth";
import { desc, eq } from "drizzle-orm";

export const prerender = false;

export const GET: APIRoute = async ({ params }) => {
	const mediaId = Number(params.id);

	if (!mediaId) return new Response("Invalid ID", { status: 400 });

	const mediaComments = await db
		.select({
			id: comments.id,
			content: comments.content,
			createdAt: comments.createdAt,
			user: {
				id: user.id,
				name: user.name,
				image: user.image,
			},
		})
		.from(comments)
		.innerJoin(user, eq(comments.userId, user.id))
		.where(eq(comments.mediaId, mediaId))
		.orderBy(desc(comments.createdAt))
		.limit(50);

	return new Response(JSON.stringify(mediaComments), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
};

export const POST: APIRoute = async ({ params, request }) => {
	const session = await auth.api.getSession({ headers: request.headers });
	if (!session?.user) {
		return new Response("Unauthorized", { status: 401 });
	}

	const mediaId = Number(params.id);
	if (!mediaId) return new Response("Invalid ID", { status: 400 });

	const body = await request.json();
	const content = body.content?.trim();

	if (!content || content.length > 1000) {
		return new Response("Invalid comment content", { status: 400 });
	}

	const newId = crypto.randomUUID();
	const now = new Date();

	await db.insert(comments).values({
		id: newId,
		userId: session.user.id,
		mediaId,
		content,
		createdAt: now,
	});

	return new Response(
		JSON.stringify({
			id: newId,
			content,
			createdAt: now,
			user: {
				id: session.user.id,
				name: session.user.name,
				image: session.user.image,
			},
		}),
		{ status: 201 },
	);
};
