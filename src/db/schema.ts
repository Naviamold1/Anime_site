import { integer, sqliteTable, text, real } from "drizzle-orm/sqlite-core";

export const media = sqliteTable("media", {
	id: integer("id").primaryKey(),
	name: text("name"),
	englishName: text("english_name"),
	japaneseName: text("japanese_name"),
	otherName: text("other_name"),
	russian: text("russian"),
	type: text("type"),
	episodes: integer("episodes"),
	episodesAired: integer("episodes_aired"),
	volumes: integer("volumes"),
	chapters: integer("chapters"),
	aired: text("aired"),
	airedOn: text("aired_on"),
	releasedOn: text("released_on"),
	premiered: text("premiered"),
	producers: text("producers"),
	licensors: text("licensors"),
	studios: text("studios"),
	source: text("source"),
	duration: text("duration"),
	rating: text("rating"),
	genres: text("genres"),
	status: text("status"),
	synopsis: text("synopsis"),
	posterUrl: text("poster_url"),
});

export type Media = typeof media.$inferSelect;

// --- Better Auth Tables ---

export const user = sqliteTable("user", {
	id: text("id").primaryKey(),
	name: text("name").notNull(),
	email: text("email").notNull().unique(),
	emailVerified: integer("email_verified", { mode: "boolean" }).notNull(),
	image: text("image"),
	onboarded: integer("onboarded", { mode: "boolean" }).default(false),
	createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
	updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
});

export const session = sqliteTable("session", {
	id: text("id").primaryKey(),
	expiresAt: integer("expires_at", { mode: "timestamp" }).notNull(),
	token: text("token").notNull().unique(),
	createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
	updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
	ipAddress: text("ip_address"),
	userAgent: text("user_agent"),
	userId: text("user_id")
		.notNull()
		.references(() => user.id),
});

export const account = sqliteTable("account", {
	id: text("id").primaryKey(),
	accountId: text("account_id").notNull(),
	providerId: text("provider_id").notNull(),
	userId: text("user_id")
		.notNull()
		.references(() => user.id),
	accessToken: text("access_token"),
	refreshToken: text("refresh_token"),
	idToken: text("id_token"),
	accessTokenExpiresAt: integer("access_token_expires_at", { mode: "timestamp" }),
	refreshTokenExpiresAt: integer("refresh_token_expires_at", { mode: "timestamp" }),
	scope: text("scope"),
	password: text("password"),
	createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
	updatedAt: integer("updated_at", { mode: "timestamp" }).notNull(),
});

export const verification = sqliteTable("verification", {
	id: text("id").primaryKey(),
	identifier: text("identifier").notNull(),
	value: text("value").notNull(),
	expiresAt: integer("expires_at", { mode: "timestamp" }).notNull(),
	createdAt: integer("created_at", { mode: "timestamp" }),
	updatedAt: integer("updated_at", { mode: "timestamp" }),
});

// --- Social Features Tables ---

export const ratings = sqliteTable("ratings", {
	id: text("id").primaryKey(),
	userId: text("user_id")
		.notNull()
		.references(() => user.id),
	mediaId: integer("media_id")
		.notNull()
		.references(() => media.id),
	score: real("score").notNull(), // e.g., 1 to 10
	createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
});

export type Rating = typeof ratings.$inferSelect;

export const comments = sqliteTable("comments", {
	id: text("id").primaryKey(),
	userId: text("user_id")
		.notNull()
		.references(() => user.id),
	mediaId: integer("media_id")
		.notNull()
		.references(() => media.id),
	content: text("content").notNull(),
	createdAt: integer("created_at", { mode: "timestamp" }).notNull(),
});

export type Comment = typeof comments.$inferSelect;
