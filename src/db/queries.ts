import { and, asc, eq, sql } from "drizzle-orm";
import { db } from "@/db/client";
import { media } from "@/db/schema";

const PAGE_SIZE = 24;

type ListArgs = {
	page?: number;
	query?: string;
	type?: string;
	status?: string;
};

type SearchArgs = {
	query?: string;
	limit?: number;
};

function normalize(input?: string) {
	return input?.trim() ?? "";
}

function listFilters(args: ListArgs) {
	const query = normalize(args.query).toLowerCase();
	const type = normalize(args.type).toLowerCase();
	const status = normalize(args.status).toLowerCase();

	const filters = [];

	if (query) {
		const pattern = `%${query}%`;
		filters.push(
			sql`(
        lower(coalesce(${media.name}, '')) like ${pattern}
        or lower(coalesce(${media.englishName}, '')) like ${pattern}
        or lower(coalesce(${media.japaneseName}, '')) like ${pattern}
      )`,
		);
	}

	if (type) {
		filters.push(sql`lower(coalesce(${media.type}, '')) = ${type}`);
	}

	if (status) {
		filters.push(sql`lower(coalesce(${media.status}, '')) = ${status}`);
	}

	return filters.length ? and(...filters) : undefined;
}

export async function getList(args: ListArgs) {
	const page = Math.max(1, Number(args.page) || 1);
	const where = listFilters(args);

	const [items, totalRows] = await Promise.all([
		db
			.select()
			.from(media)
			.where(where)
			.orderBy(asc(media.id))
			.limit(PAGE_SIZE)
			.offset((page - 1) * PAGE_SIZE),
		db
			.select({ value: sql<number>`count(*)` })
			.from(media)
			.where(where),
	]);

	const total = totalRows[0]?.value ?? 0;
	return {
		items,
		page,
		pageSize: PAGE_SIZE,
		total,
		totalPages: Math.max(1, Math.ceil(total / PAGE_SIZE)),
	};
}

export async function getMediaById(id: number) {
	const rows = await db.select().from(media).where(eq(media.id, id)).limit(1);
	return rows[0] ?? null;
}

export async function searchMedia(args: SearchArgs) {
	const query = normalize(args.query).toLowerCase();
	const limit = Math.min(100, Math.max(1, Number(args.limit) || 24));

	if (!query) {
		return [];
	}

	const pattern = `%${query}%`;
	return db
		.select({
			id: media.id,
			name: media.name,
			englishName: media.englishName,
			japaneseName: media.japaneseName,
			type: media.type,
			status: media.status,
			episodes: media.episodes,
			genres: media.genres,
			posterUrl: media.posterUrl,
		})
		.from(media)
		.where(
			sql`(
        lower(coalesce(${media.name}, '')) like ${pattern}
        or lower(coalesce(${media.englishName}, '')) like ${pattern}
        or lower(coalesce(${media.japaneseName}, '')) like ${pattern}
        or lower(coalesce(${media.genres}, '')) like ${pattern}
      )`,
		)
		.orderBy(asc(media.id))
		.limit(limit);
}

export async function getFacetOptions() {
	const [types, statuses] = await Promise.all([
		db
			.select({
				value: media.type,
				count: sql<number>`count(*)`,
			})
			.from(media)
			.where(sql`${media.type} is not null and ${media.type} != ''`)
			.groupBy(media.type)
			.orderBy(sql`count(*) desc`)
			.limit(30),
		db
			.select({
				value: media.status,
				count: sql<number>`count(*)`,
			})
			.from(media)
			.where(sql`${media.status} is not null and ${media.status} != ''`)
			.groupBy(media.status)
			.orderBy(sql`count(*) desc`)
			.limit(30),
	]);

	return {
		types,
		statuses,
	};
}

export async function getMediaByCategory(category: string, limit = 20) {
	let condition = undefined;

	switch (category.toLowerCase()) {
		case "trending":
			// Approximation for trending: recent release, high episodes, TV type
			condition = and(
				eq(sql`lower(${media.type})`, "tv"),
				sql`${media.status} = 'Currently Airing'`,
			);
			break;
		case "top romance":
			condition = sql`lower(${media.genres}) like '%romance%'`;
			break;
		case "action packed":
			condition = sql`lower(${media.genres}) like '%action%'`;
			break;
		case "popular specials":
			condition = sql`lower(${media.type}) = 'special'`;
			break;
		case "movies":
			condition = sql`lower(${media.type}) = 'movie'`;
			break;
		default:
			condition = sql`1=1`;
	}

	return db.select().from(media).where(condition).orderBy(asc(media.id)).limit(limit);
}

export async function getFeaturedMedia() {
	// Let's pick a specific highly rated or popular anime that has a good poster.
	// We'll try to find one with a high episode count or known popular name, or just fallback to id=1
	const rows = await db
		.select()
		.from(media)
		.where(sql`lower(${media.type}) = 'tv' and ${media.posterUrl} is not null`)
		// try to get something in the middle or randomize to some extent, but order by episodes desc as a proxy for popularity for now
		.orderBy(sql`RANDOM()`)
		.limit(1);

	return rows[0] ?? null;
}
